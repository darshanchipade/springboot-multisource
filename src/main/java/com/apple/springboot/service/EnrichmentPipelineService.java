package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class EnrichmentPipelineService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentPipelineService.class);

    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;
    private final ConsolidatedSectionService consolidatedSectionService;
    private final TextChunkingService textChunkingService;
    private final ContentChunkRepository contentChunkRepository;
    private final TransactionalEnrichmentService transactionalEnrichmentService;
    private final Executor enrichmentTaskExecutor;
    private final BedrockEnrichmentService bedrockEnrichmentService;


    public EnrichmentPipelineService(CleansedDataStoreRepository cleansedDataStoreRepository,
                                     ObjectMapper objectMapper,
                                     ConsolidatedSectionService consolidatedSectionService,
                                     TextChunkingService textChunkingService,
                                     ContentChunkRepository contentChunkRepository,
                                     TransactionalEnrichmentService transactionalEnrichmentService,
                                     @Qualifier("enrichmentTaskExecutor") Executor enrichmentTaskExecutor,
                                     BedrockEnrichmentService bedrockEnrichmentService) {
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
        this.consolidatedSectionService = consolidatedSectionService;
        this.textChunkingService = textChunkingService;
        this.contentChunkRepository = contentChunkRepository;
        this.transactionalEnrichmentService = transactionalEnrichmentService;
        this.enrichmentTaskExecutor = enrichmentTaskExecutor;
        this.bedrockEnrichmentService = bedrockEnrichmentService;
    }

    @Transactional
    public void enrichAndStore(CleansedDataStore cleansedDataEntry) throws JsonProcessingException {
        if (cleansedDataEntry == null || cleansedDataEntry.getId() == null) {
            logger.warn("Received null or no ID CleansedDataStore entry for enrichment. Skipping...");
            return;
        }

        UUID cleansedDataStoreId = cleansedDataEntry.getId();
        logger.info("Starting enrichment process for CleansedDataStore ID: {}", cleansedDataStoreId);

        if (!"CLEANSED_PENDING_ENRICHMENT".equals(cleansedDataEntry.getStatus())) {
            logger.info("CleansedDataStore ID: {} is not in 'CLEANSED_PENDING_ENRICHMENT' state (current: {}). Skipping enrichment.",
                    cleansedDataStoreId, cleansedDataEntry.getStatus());
            return;
        }

        cleansedDataEntry.setStatus("ENRICHMENT_IN_PROGRESS");
        cleansedDataStoreRepository.save(cleansedDataEntry);

        List<Map<String, Object>> maps = cleansedDataEntry.getCleansedItems();
        if (maps == null || maps.isEmpty()) {
            logger.info("No items found in cleansed_items for CleansedDataStore ID: {}. Marking as ENRICHED_NO_ITEMS_TO_PROCESS.", cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHED_NO_ITEMS_TO_PROCESS");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            return;
        }

        List<CleansedItemDetail> itemsToEnrich = convertMapsToCleansedItemDetails(maps);

        // Step 1: Loop and enrich all elements in parallel
        List<CompletableFuture<EnrichmentResult>> futures = itemsToEnrich.stream()
                .map(itemDetail -> CompletableFuture.supplyAsync(
                        () -> transactionalEnrichmentService.enrichItem(itemDetail, cleansedDataEntry),
                        enrichmentTaskExecutor))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger skippedByRateLimitCount = new AtomicInteger(0);
        List<String> itemProcessingErrors = new ArrayList<>();

        futures.forEach(future -> {
            try {
                EnrichmentResult result = future.get();
                if (result.isSuccess()) {
                    successCount.incrementAndGet();
                } else if (result.isFailure()) {
                    failureCount.incrementAndGet();
                    result.getErrorMessage().ifPresent(itemProcessingErrors::add);
                } else if (result.isRateLimited()) {
                    skippedByRateLimitCount.incrementAndGet();
                    result.getErrorMessage().ifPresent(itemProcessingErrors::add);
                }
            } catch (Exception e) {
                logger.error("Error processing enrichment result future", e);
                failureCount.incrementAndGet();
                itemProcessingErrors.add("An unexpected error occurred while processing an enrichment result.");
            }
        });


        // Step 2: Consolidate sections once after all enrichments are saved
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);

        // Step 3: Create content chunks once after consolidation
        List<ConsolidatedEnrichedSection> savedSections = consolidatedSectionService.getSectionsFor(cleansedDataEntry);
        for (ConsolidatedEnrichedSection section : savedSections) {
            List<String> chunks = textChunkingService.chunkIfNeeded(section.getCleansedText());
            for (String chunkText : chunks) {
                try {
                    float[] vector = bedrockEnrichmentService.generateEmbedding(chunkText);
                    ContentChunk contentChunk = new ContentChunk();
                    contentChunk.setConsolidatedEnrichedSection(section);
                    contentChunk.setChunkText(chunkText);
                    contentChunk.setSourceField(section.getSourceUri());
                    contentChunk.setSectionPath(section.getSectionPath());
                    contentChunk.setVector(vector);
                    contentChunk.setCreatedAt(OffsetDateTime.now());
                    contentChunk.setCreatedBy("EnrichmentPipelineService");
                    contentChunkRepository.save(contentChunk);
                } catch (Exception e) {
                    logger.error("Error creating content chunk for item path {}: {}", section.getSectionPath(), e.getMessage(), e);
                    // Log and continue to the next chunk
                }
            }
        }

        // Step 4: Update final status
        updateFinalCleansedDataStatus(cleansedDataEntry, successCount.get(), failureCount.get(), skippedByRateLimitCount.get(), itemsToEnrich.size(), itemProcessingErrors);
    }

    private List<CleansedItemDetail> convertMapsToCleansedItemDetails(List<Map<String, Object>> maps) {
        return maps.stream()
                .map(map -> {
                    try {
                        String sourcePath = (String) map.get("sourcePath");
                        String originalFieldName = (String) map.get("originalFieldName");
                        String cleansedContent = (String) map.get("cleansedContent");
                        String model = (String) map.get("model");
                        EnrichmentContext context = objectMapper.convertValue(map.get("context"), EnrichmentContext.class);
                        return new CleansedItemDetail(sourcePath, originalFieldName, cleansedContent, model, context);
                    } catch (Exception e) {
                        logger.warn("Could not convert map to CleansedItemDetail object. Skipping item. Map: {}, Error: {}", map, e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private void updateFinalCleansedDataStatus(CleansedDataStore cleansedDataEntry, int successCount, int failureCount, int skippedByRateLimitCount, int totalItems, List<String> itemProcessingErrors) {
        String finalStatus;
        if (failureCount == 0 && skippedByRateLimitCount == 0 && successCount == totalItems) {
            finalStatus = "ENRICHED_COMPLETE";
        } else if (successCount > 0) {
            finalStatus = "PARTIALLY_ENRICHED";
        } else {
            finalStatus = "ENRICHMENT_FAILED";
        }
        cleansedDataEntry.setStatus(finalStatus);

        Map<String, Object> summary = new HashMap<>();
        summary.put("totalItems", totalItems);
        summary.put("successfullyEnriched", successCount);
        summary.put("failedEnrichment", failureCount);
        summary.put("skippedByRateLimit", skippedByRateLimitCount);
        summary.put("errors", itemProcessingErrors);
        try {
            cleansedDataEntry.setCleansingErrors(summary);
        } catch (Exception e) {
            logger.error("Could not set processing summary", e);
        }

        cleansedDataStoreRepository.save(cleansedDataEntry);
        logger.info("Finished enrichment for CleansedDataStore ID: {}. Final status: {}", cleansedDataEntry.getId(), finalStatus);
    }
}