package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.ratelimiter.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
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
    private final RateLimiter bedrockEnricherRateLimiter;
    private final JobProgressService jobProgressService;

    @Value("${enrichment.batch.size:10}")
    private int batchSize;

    public EnrichmentPipelineService(CleansedDataStoreRepository cleansedDataStoreRepository,
                                     ObjectMapper objectMapper,
                                     ConsolidatedSectionService consolidatedSectionService,
                                     TextChunkingService textChunkingService,
                                     ContentChunkRepository contentChunkRepository,
                                     TransactionalEnrichmentService transactionalEnrichmentService,
                                     @Qualifier("enrichmentTaskExecutor") Executor enrichmentTaskExecutor,
                                     BedrockEnrichmentService bedrockEnrichmentService,
                                     @Qualifier("bedrockEnricherRateLimiter") RateLimiter bedrockEnricherRateLimiter,
                                     JobProgressService jobProgressService) {
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
        this.consolidatedSectionService = consolidatedSectionService;
        this.textChunkingService = textChunkingService;
        this.contentChunkRepository = contentChunkRepository;
        this.transactionalEnrichmentService = transactionalEnrichmentService;
        this.enrichmentTaskExecutor = enrichmentTaskExecutor;
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.bedrockEnricherRateLimiter = bedrockEnricherRateLimiter;
        this.jobProgressService = jobProgressService;
    }

    public void enrichAndStore(CleansedDataStore cleansedDataEntry, String jobId) throws JsonProcessingException {
        jobProgressService.updateProgress(jobId, "Enrichment pipeline started.");
        if (cleansedDataEntry == null || cleansedDataEntry.getId() == null) {
            logger.warn("Received null or no ID CleansedDataStore entry for enrichment. Skipping...");
            jobProgressService.updateProgress(jobId, "ERROR: Received null CleansedDataStore entry. Aborting.");
            jobProgressService.completeJob(jobId);
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
        jobProgressService.updateProgress(jobId, "Found " + itemsToEnrich.size() + " items to process. Grouping into batches of " + batchSize);

        List<List<CleansedItemDetail>> batches = new ArrayList<>();
        for (int i = 0; i < itemsToEnrich.size(); i += batchSize) {
            batches.add(itemsToEnrich.subList(i, Math.min(i + batchSize, itemsToEnrich.size())));
        }

        List<CompletableFuture<EnrichmentResult>> futures = new ArrayList<>();
        for (List<CleansedItemDetail> batch : batches) {
            CompletableFuture<EnrichmentResult> future = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            // First, try to process as a batch
                            return transactionalEnrichmentService.enrichBatch(batch, cleansedDataEntry, jobId);
                        } catch (Exception e) {
                            // Fallback to single-item processing if batch fails
                            logger.warn("Batch enrichment failed for a batch of size {}. Falling back to single-item processing.", batch.size(), e);
                            List<EnrichedContentElement> successfulElements = new ArrayList<>();
                            for (CleansedItemDetail item : batch) {
                                try {
                                    EnrichmentResult singleResult = transactionalEnrichmentService.enrichItem(item, cleansedDataEntry, jobId);
                                    if (singleResult.isSuccess()) {
                                        successfulElements.addAll(singleResult.getEnrichedContentElements());
                                    }
                                } catch (Exception singleItemException) {
                                    logger.error("Single-item enrichment failed during fallback for item: {}", item.sourcePath, singleItemException);
                                    jobProgressService.updateProgress(jobId, "FAILED (in fallback): " + item.sourcePath + " - " + singleItemException.getMessage());
                                }
                            }
                            return EnrichmentResult.success(successfulElements);
                        }
                    },
                    enrichmentTaskExecutor
            );
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<EnrichedContentElement> successfullyEnrichedItems = new ArrayList<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger skippedByRateLimitCount = new AtomicInteger(0);
        List<String> itemProcessingErrors = new ArrayList<>();

        futures.forEach(future -> {
            try {
                EnrichmentResult result = future.get();
                if (result.isSuccess()) {
                    successfullyEnrichedItems.addAll(result.getEnrichedContentElements());
                    successCount.addAndGet(result.getEnrichedContentElements().size());
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

        jobProgressService.updateProgress(jobId, "Consolidating " + successfullyEnrichedItems.size() + " enriched items.");
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);

        List<ConsolidatedEnrichedSection> savedSections = consolidatedSectionService.getSectionsFor(cleansedDataEntry);
        jobProgressService.updateProgress(jobId, "Creating content chunks from " + savedSections.size() + " consolidated sections.");
        createContentChunksInBatch(savedSections, itemProcessingErrors);

        jobProgressService.updateProgress(jobId, "Updating final status.");
        updateFinalCleansedDataStatus(cleansedDataEntry, successCount.get(), failureCount.get(), skippedByRateLimitCount.get(), itemsToEnrich.size(), itemProcessingErrors);
        jobProgressService.updateProgress(jobId, "Pipeline finished with status: " + cleansedDataEntry.getStatus());
        jobProgressService.completeJob(jobId);
    }

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
        } catch (Exception e) {
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