package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.ratelimiter.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class EnrichmentPipelineService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentPipelineService.class);
    private static final int BATCH_SIZE = 50; // Adjust based on Bedrock quotas

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

    @Bulkhead(name = "bedrockBulkhead")
    public void enrichAndStore(CleansedDataStore cleansedDataEntry) throws JsonProcessingException {
        if (cleansedDataEntry == null || cleansedDataEntry.getId() == null) {
            logger.warn("Received null or no ID CleansedDataStore entry. Skipping...");
            return;
        }

        UUID cleansedDataStoreId = cleansedDataEntry.getId();
        logger.info("Starting enrichment for CleansedDataStore ID: {}", cleansedDataStoreId);

        if (!"CLEANSED_PENDING_ENRICHMENT".equals(cleansedDataEntry.getStatus())) {
            logger.info("CleansedDataStore ID: {} is not in 'CLEANSED_PENDING_ENRICHMENT' state (current: {}). Skipping.",
                    cleansedDataStoreId, cleansedDataEntry.getStatus());
            return;
        }

        cleansedDataEntry.setStatus("ENRICHMENT_IN_PROGRESS");
        cleansedDataStoreRepository.save(cleansedDataEntry);

        List<Map<String, Object>> maps = cleansedDataEntry.getCleansedItems();
        if (maps == null || maps.isEmpty()) {
            logger.info("No items found for CleansedDataStore ID: {}. Marking as ENRICHED_NO_ITEMS_TO_PROCESS.", cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHED_NO_ITEMS_TO_PROCESS");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            return;
        }

        logger.info("Total cleansed items before filtering for CleansedDataStore ID {}: {}", cleansedDataStoreId, maps.size());
        // Log sample items for debugging (limit to 5 to avoid log spam)
        logger.debug("Sample of cleansed items (up to 5): {}", maps.size() > 5 ? maps.subList(0, 5) : maps);
        List<CleansedItemDetail> itemsToEnrich = convertMapsToCleansedItemDetails(maps);
        logger.info("Total valid items to enrich for CleansedDataStore ID {}: {}", cleansedDataStoreId, itemsToEnrich.size());

        if (itemsToEnrich.isEmpty()) {
            logger.info("No valid items after filtering for CleansedDataStore ID: {}. Marking as ENRICHED_NO_ITEMS_TO_PROCESS.", cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHED_NO_ITEMS_TO_PROCESS");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            return;
        }

        AtomicInteger batchId = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger skippedByRateLimitCount = new AtomicInteger(0);
        List<String> itemProcessingErrors = new ArrayList<>();

        // Split into batches and process synchronously
        List<List<CleansedItemDetail>> batchLists = new ArrayList<>();
        for (int i = 0; i < itemsToEnrich.size(); i += BATCH_SIZE) {
            batchLists.add(itemsToEnrich.subList(i, Math.min(i + BATCH_SIZE, itemsToEnrich.size())));
        }

        for (List<CleansedItemDetail> batch : batchLists) {
            int currentBatchId = batchId.incrementAndGet();
            logger.info("Processing batch {} with {} items for CleansedDataStore ID: {}",
                    currentBatchId, batch.size(), cleansedDataStoreId);
            EnrichmentResult result = processBatch(batch, cleansedDataEntry, currentBatchId);
            if (result.isSuccess()) {
                successCount.addAndGet(batch.size());
            } else if (result.isFailure()) {
                failureCount.addAndGet(batch.size());
                result.getErrorMessage().ifPresent(itemProcessingErrors::add);
            } else if (result.isRateLimited()) {
                skippedByRateLimitCount.addAndGet(batch.size());
                result.getErrorMessage().ifPresent(itemProcessingErrors::add);
            }
        }

        // Consolidate sections and create chunks
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);
        List<ConsolidatedEnrichedSection> savedSections = consolidatedSectionService.getSectionsFor(cleansedDataEntry);
        createContentChunksInBatch(savedSections, itemProcessingErrors);

        // Update final status
        updateFinalCleansedDataStatus(cleansedDataEntry, successCount.get(), failureCount.get(), skippedByRateLimitCount.get(), itemsToEnrich.size(), itemProcessingErrors);
    }

    @Bulkhead(name = "bedrockBulkhead")
    public void enrichAndStoreSingleItem(CleansedDataStore cleansedDataEntry, Map<String, Object> updatedItem) throws JsonProcessingException {
        if (cleansedDataEntry == null || cleansedDataEntry.getId() == null) {
            logger.warn("Received null or no ID CleansedDataStore entry for single item update. Skipping...");
            return;
        }
        if (updatedItem == null) {
            logger.warn("Received null updated item for CleansedDataStore ID: {}. Skipping...", cleansedDataEntry.getId());
            return;
        }

        UUID cleansedDataStoreId = cleansedDataEntry.getId();
        logger.info("Starting single-item enrichment for CleansedDataStore ID: {}", cleansedDataStoreId);
        logger.debug("Updated item: {}", updatedItem);

        if (!"CLEANSED_PENDING_ENRICHMENT".equals(cleansedDataEntry.getStatus())) {
            logger.info("CleansedDataStore ID: {} is not in 'CLEANSED_PENDING_ENRICHMENT' state (current: {}). Skipping.",
                    cleansedDataStoreId, cleansedDataEntry.getStatus());
            return;
        }

        cleansedDataEntry.setStatus("ENRICHMENT_IN_PROGRESS");
        cleansedDataStoreRepository.save(cleansedDataEntry);

        // Process only the updated item
        List<CleansedItemDetail> itemsToEnrich = convertMapsToCleansedItemDetails(Collections.singletonList(updatedItem));
        logger.info("Total valid items to enrich for CleansedDataStore ID {}: {}", cleansedDataStoreId, itemsToEnrich.size());

        if (itemsToEnrich.isEmpty()) {
            logger.warn("No valid items after filtering single updated item for CleansedDataStore ID: {}. Marking as ENRICHED_NO_ITEMS_TO_PROCESS.", cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHED_NO_ITEMS_TO_PROCESS");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            return;
        }

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger skippedByRateLimitCount = new AtomicInteger(0);
        List<String> itemProcessingErrors = new ArrayList<>();

        // Process single item as a batch of 1
        EnrichmentResult result = processBatch(itemsToEnrich, cleansedDataEntry, 1);
        if (result.isSuccess()) {
            successCount.incrementAndGet();
        } else if (result.isFailure()) {
            failureCount.incrementAndGet();
            result.getErrorMessage().ifPresent(itemProcessingErrors::add);
        } else if (result.isRateLimited()) {
            skippedByRateLimitCount.incrementAndGet();
            result.getErrorMessage().ifPresent(itemProcessingErrors::add);
        }

        // Update cleansedItems to include the processed item
        List<Map<String, Object>> cleansedItems = cleansedDataEntry.getCleansedItems();
        if (cleansedItems == null) {
            cleansedItems = new ArrayList<>();
        }
        String sourcePath = (String) updatedItem.get("sourcePath");
        String originalFieldName = (String) updatedItem.get("originalFieldName");
        cleansedItems.removeIf(item -> sourcePath.equals(item.get("sourcePath")) &&
                originalFieldName.equals(item.get("originalFieldName")));
        cleansedItems.add(updatedItem);
        cleansedDataEntry.setCleansedItems(cleansedItems);
        cleansedDataStoreRepository.save(cleansedDataEntry);

        // Consolidate sections and create chunks
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);
        List<ConsolidatedEnrichedSection> savedSections = consolidatedSectionService.getSectionsFor(cleansedDataEntry);
        createContentChunksInBatch(savedSections, itemProcessingErrors);

        // Update final status
        updateFinalCleansedDataStatus(cleansedDataEntry, successCount.get(), failureCount.get(), skippedByRateLimitCount.get(), itemsToEnrich.size(), itemProcessingErrors);
    }

    private EnrichmentResult processBatch(List<CleansedItemDetail> batch, CleansedDataStore cleansedDataEntry, int batchId) {
        try {
            return RateLimiter.decorateSupplier(
                    bedrockEnricherRateLimiter,
                    () -> {
                        logger.info("Processing batch {} with {} items for CleansedDataStore ID: {}",
                                batchId, batch.size(), cleansedDataEntry.getId());
                        return transactionalEnrichmentService.enrichBatch(batch, cleansedDataEntry);
                    }
            ).get();
        } catch (Exception e) {
            logger.error("Error processing batch {} for CleansedDataStore ID {}: {}",
                    batchId, cleansedDataEntry.getId(), e.getMessage(), e);
            return EnrichmentResult.failure("Batch processing error: " + e.getMessage());
        }
    }

    private List<CleansedItemDetail> convertMapsToCleansedItemDetails(List<Map<String, Object>> maps) {
        List<CleansedItemDetail> items = new ArrayList<>();
        int invalidItems = 0;
        for (Map<String, Object> map : maps) {
            try {
                String sourcePath = (String) map.get("sourcePath");
                String originalFieldName = (String) map.get("originalFieldName");
                String cleansedContent = (String) map.get("cleansedContent");
                String model = (String) map.get("model");
                EnrichmentContext context = map.get("context") != null ?
                        objectMapper.convertValue(map.get("context"), EnrichmentContext.class) : null;

                if (sourcePath == null || sourcePath.trim().isEmpty()) {
                    logger.warn("Skipping item due to null/empty sourcePath. Map: {}", map);
                    invalidItems++;
                    continue;
                }
                if (originalFieldName == null || originalFieldName.trim().isEmpty()) {
                    logger.warn("Skipping item due to null/empty originalFieldName. Map: {}", map);
                    invalidItems++;
                    continue;
                }
                if (cleansedContent == null || cleansedContent.trim().isEmpty()) {
                    logger.warn("Skipping item due to null/empty cleansedContent. Map: {}", map);
                    invalidItems++;
                    continue;
                }

                items.add(new CleansedItemDetail(sourcePath, originalFieldName, cleansedContent, model, context));
            } catch (Exception e) {
                logger.warn("Could not convert map to CleansedItemDetail. Skipping item. Map: {}, Error: {}", map, e.getMessage());
                invalidItems++;
            }
        }
        if (invalidItems > 0) {
            logger.info("Filtered out {} invalid items during conversion for CleansedDataStore ID: {}",
                    invalidItems, maps.isEmpty() ? "unknown" : maps.get(0).get("cleansedDataStoreId"));
        }
        return items;
    }

    private void createContentChunksInBatch(List<ConsolidatedEnrichedSection> sections, List<String> itemProcessingErrors) {
        if (sections == null || sections.isEmpty()) {
            logger.info("No sections to process for chunking.");
            return;
        }

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
                String warn = String.format("Chunk/embedding size mismatch: chunks=%d, embeddings=%d. Saved first %d.", placeholders.size(), vectors.size(), saved);
                logger.warn(warn);
                itemProcessingErrors.add(warn);
            }

            logger.info("Saved {} content chunks (requested {}).", saved, placeholders.size());
        } catch (Exception e) {
            String msg = "Embedding step failed; no chunks saved. " + e.getClass().getSimpleName() + ": " + e.getMessage();
            logger.error(msg, e);
            itemProcessingErrors.add(msg);
        }
    }

    private void updateFinalCleansedDataStatus(CleansedDataStore cleansedDataEntry, int successCount, int failureCount, int skippedByRateLimitCount, int totalItems, List<String> itemProcessingErrors) {
        String finalStatus;
        if (failureCount == 0 && skippedByRateLimitCount == 0 && successCount >= totalItems) {
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
            logger.error("Could not set processing summary for CleansedDataStore ID {}: {}", cleansedDataEntry.getId(), e.getMessage());
        }

        cleansedDataStoreRepository.save(cleansedDataEntry);
        logger.info("Finished enrichment for CleansedDataStore ID: {}. Final status: {}", cleansedDataEntry.getId(), finalStatus);
    }
}