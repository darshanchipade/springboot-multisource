package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    private final RateLimiter bedrockEnricherRateLimiter;


    public EnrichmentPipelineService(CleansedDataStoreRepository cleansedDataStoreRepository,
                                     ObjectMapper objectMapper,
                                     ConsolidatedSectionService consolidatedSectionService,
                                     TextChunkingService textChunkingService,
                                     ContentChunkRepository contentChunkRepository,
                                     TransactionalEnrichmentService transactionalEnrichmentService,
                                     @Qualifier("enrichmentTaskExecutor") Executor enrichmentTaskExecutor,
                                     BedrockEnrichmentService bedrockEnrichmentService,
                                     @Qualifier("bedrockEnricherRateLimiter") RateLimiter bedrockEnricherRateLimiter) {
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
        this.consolidatedSectionService = consolidatedSectionService;
        this.textChunkingService = textChunkingService;
        this.contentChunkRepository = contentChunkRepository;
        this.transactionalEnrichmentService = transactionalEnrichmentService;
        this.enrichmentTaskExecutor = enrichmentTaskExecutor;
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.bedrockEnricherRateLimiter = bedrockEnricherRateLimiter;
    }

    //@Transactional
    @Bulkhead(name = "bedrockBulkhead")
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
                        RateLimiter.decorateSupplier(
                                bedrockEnricherRateLimiter,
                                () -> transactionalEnrichmentService.enrichItem(itemDetail, cleansedDataEntry)
                        ),
                        enrichmentTaskExecutor
                ))
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
        createContentChunksInBatch(savedSections, itemProcessingErrors);


        // Step 4: Update final status
        updateFinalCleansedDataStatus(cleansedDataEntry, successCount.get(), failureCount.get(), skippedByRateLimitCount.get(), itemsToEnrich.size(), itemProcessingErrors);
    }

//    private void createContentChunksInBatch(List<ConsolidatedEnrichedSection> sections, List<String> itemProcessingErrors) {
//        if (sections == null || sections.isEmpty()) {
//            return;
//        }
//
//        List<String> allChunkTexts = new ArrayList<>();
//        List<ContentChunk> chunkPlaceholders = new ArrayList<>();
//
//        for (ConsolidatedEnrichedSection section : sections) {
//            List<String> chunks = textChunkingService.chunkIfNeeded(section.getCleansedText());
//            for (String chunkText : chunks) {
//                allChunkTexts.add(chunkText);
//
//                ContentChunk placeholder = new ContentChunk();
//                placeholder.setConsolidatedEnrichedSection(section);
//                placeholder.setChunkText(chunkText);
//                placeholder.setSourceField(section.getSourceUri());
//                placeholder.setSectionPath(section.getSectionPath());
//                placeholder.setCreatedAt(OffsetDateTime.now());
//                placeholder.setCreatedBy("EnrichmentPipelineService");
//                chunkPlaceholders.add(placeholder);
//            }
//        }
//
//        if (allChunkTexts.isEmpty()) {
//            return;
//        }
//
//        try {
//            List<float[]> embeddings = bedrockEnrichmentService.generateEmbeddingsInBatch(allChunkTexts);
//
//            if (embeddings.size() == chunkPlaceholders.size()) {
//                IntStream.range(0, chunkPlaceholders.size()).forEach(i -> {
//                    ContentChunk chunk = chunkPlaceholders.get(i);
//                    chunk.setVector(embeddings.get(i));
//                    contentChunkRepository.save(chunk);
//                });
//                logger.info("Successfully created and saved {} content chunks in a batch.", chunkPlaceholders.size());
//            } else {
//                String errorMessage = String.format("Mismatch between number of chunks (%d) and generated embeddings (%d). Aborting chunk saving.",
//                        chunkPlaceholders.size(), embeddings.size());
//                logger.error(errorMessage);
//                itemProcessingErrors.add(errorMessage);
//            }
//        } catch (RequestNotPermitted rnp) {
//            String errorMessage = "Rate limit exceeded during batch embedding generation. No content chunks were created.";
//            logger.warn(errorMessage, rnp);
//            itemProcessingErrors.add(errorMessage);
//        } catch (IOException e) {
//            String errorMessage = "Failed to generate embeddings for content chunks due to Bedrock API error.";
//            logger.error(errorMessage, e);
//            itemProcessingErrors.add(errorMessage);
//        }
//    }



    private void createContentChunksInBatch(List<ConsolidatedEnrichedSection> sections, List<String> itemProcessingErrors) {
        if (sections == null || sections.isEmpty()) return;

        List<String> allChunkTexts = new ArrayList<>();
        List<ContentChunk> placeholders = new ArrayList<>();

        for (ConsolidatedEnrichedSection section : sections) {
            String text = section.getCleansedText();
            if (text == null || text.isBlank()) {
                logger.debug("Section {} has empty cleansedText; skipping chunking.", section.getSectionPath());
                continue;
            }
            List<String> chunks = textChunkingService.chunkIfNeeded(text);
            if (chunks == null || chunks.isEmpty()) continue;

            for (String chunkText : chunks) {
                allChunkTexts.add(chunkText);
                ContentChunk p = new ContentChunk();
                p.setConsolidatedEnrichedSection(section);
                p.setChunkText(chunkText);
                p.setSourceField(section.getSourceUri());
                p.setSectionPath(section.getSectionPath());
                p.setCreatedAt(OffsetDateTime.now());
                p.setCreatedBy("EnrichmentPipelineService");
                placeholders.add(p);
            }
        }

        if (allChunkTexts.isEmpty()) {
            logger.info("No chunk texts to embed; skipping chunk creation.");
            return;
        }

        try {
            List<float[]> vectors = bedrockEnrichmentService.generateEmbeddingsInBatch(allChunkTexts);

            int saved = 0;
            int n = Math.min(placeholders.size(), vectors.size());
            for (int i = 0; i < n; i++) {
                try {
                    ContentChunk chunk = placeholders.get(i);
                    chunk.setVector(vectors.get(i));
                    contentChunkRepository.save(chunk);
                    saved++;
                } catch (Exception e) {
                    logger.warn("Failed to save chunk index {} path {}: {}", i, placeholders.get(i).getSectionPath(), e.toString());
                }
            }

            if (vectors.size() != placeholders.size()) {
                String warn = String.format(
                        "Chunk/embedding size mismatch: chunks=%d, embeddings=%d. Saved first %d.",
                        placeholders.size(), vectors.size(), saved
                );
                logger.warn(warn);
                itemProcessingErrors.add(warn);
            }

            logger.info("Saved {} content chunks (requested {}).", saved, placeholders.size());
        } catch (Exception e) { // catch all, including BedrockRuntimeException
            String msg = "Embedding step failed; no chunks saved. " + e.getClass().getSimpleName() + ": " + e.getMessage();
            logger.error(msg, e);
            itemProcessingErrors.add(msg);
        }
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