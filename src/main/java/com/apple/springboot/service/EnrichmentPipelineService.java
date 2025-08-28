package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class EnrichmentPipelineService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentPipelineService.class);

    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final EnrichedContentElementRepository enrichedContentElementRepository;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;
    private final ConsolidatedSectionService consolidatedSectionService;
    private final AIResponseValidator aiResponseValidator;
    private final TextChunkingService textChunkingService;
    private final ContentChunkRepository contentChunkRepository;

    public EnrichmentPipelineService(BedrockEnrichmentService bedrockEnrichmentService,
                                     EnrichedContentElementRepository enrichedContentElementRepository,
                                     CleansedDataStoreRepository cleansedDataStoreRepository,
                                     ObjectMapper objectMapper,
                                     ConsolidatedSectionService consolidatedSectionService,
                                     AIResponseValidator aiResponseValidator,
                                     TextChunkingService textChunkingService,
                                     ContentChunkRepository contentChunkRepository) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
        this.consolidatedSectionService = consolidatedSectionService;
        this.aiResponseValidator = aiResponseValidator;
        this.textChunkingService = textChunkingService;
        this.contentChunkRepository = contentChunkRepository;
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
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger skippedByRateLimitCount = new AtomicInteger(0);
        List<String> itemProcessingErrors = new ArrayList<>();

        // Step 1: Loop and enrich all elements
        for (CleansedItemDetail itemDetail : itemsToEnrich) {
            if (itemDetail.cleansedContent == null || itemDetail.cleansedContent.trim().isEmpty()) {
                logger.warn("Skipping enrichment for item in CleansedDataStore ID: {} (path: {}) due to empty cleansed text.", cleansedDataStoreId, itemDetail.sourcePath);
                continue;
            }

            try {
                Map<String, String> itemContent = new HashMap<>();
                itemContent.put("cleansedContent", itemDetail.cleansedContent);
                JsonNode itemContentAsJson = objectMapper.valueToTree(itemContent);

                Map<String, Object> enrichmentResultsFromBedrock = bedrockEnrichmentService.enrichItem(itemContentAsJson, itemDetail.context);

                if (enrichmentResultsFromBedrock.containsKey("error")) {
                    throw new RuntimeException("Bedrock enrichment failed: " + enrichmentResultsFromBedrock.get("error"));
                }

                // Restore the logic to add required metadata before validation.
                Map<String, Object> contextMap = objectMapper.convertValue(itemDetail.context, new com.fasterxml.jackson.core.type.TypeReference<>() {});
                String fullContextId = itemDetail.sourcePath + "::" + itemDetail.originalFieldName;
                contextMap.put("fullContextId", fullContextId);
                contextMap.put("sourcePath", itemDetail.sourcePath);
                Map<String, Object> provenance = new HashMap<>();
                provenance.put("modelId", bedrockEnrichmentService.getConfiguredModelId());
                contextMap.put("provenance", provenance);
                enrichmentResultsFromBedrock.put("context", contextMap);

                if (!aiResponseValidator.isValid(enrichmentResultsFromBedrock)) {
                    throw new RuntimeException("Validation failed for AI response structure. Check logs for details: " + objectMapper.writeValueAsString(enrichmentResultsFromBedrock));
                }

                saveEnrichedElement(itemDetail, cleansedDataEntry, enrichmentResultsFromBedrock, "ENRICHED");
                successCount.incrementAndGet();
            } catch (RequestNotPermitted rnp) {
                logger.warn("Rate limit exceeded for Bedrock call (CleansedDataStore ID: {}, item path: {}). Item skipped.", cleansedDataStoreId, itemDetail.sourcePath, rnp);
                saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_RATE_LIMITED", rnp.getMessage());
                skippedByRateLimitCount.incrementAndGet();
                itemProcessingErrors.add(String.format("Item '%s': Rate limited - %s", itemDetail.sourcePath, rnp.getMessage()));
            } catch (Exception e) {
                logger.error("Error during enrichment for item (CleansedDataStore ID: {}, item path: {}): {}", cleansedDataStoreId, itemDetail.sourcePath, e.getMessage(), e);
                saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_ENRICHMENT_FAILED", e.getMessage());
                failureCount.incrementAndGet();
                itemProcessingErrors.add(String.format("Item '%s': Failed - %s", itemDetail.sourcePath, e.getMessage()));
            }
        }

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

    private void saveEnrichedElement(CleansedItemDetail itemDetail, CleansedDataStore parentEntry,
                                     Map<String, Object> bedrockResponse, String elementStatus) throws JsonProcessingException {
        EnrichedContentElement enrichedElement = new EnrichedContentElement();
        enrichedElement.setCleansedDataId(parentEntry.getId());
        enrichedElement.setVersion(parentEntry.getVersion());
        enrichedElement.setSourceUri(parentEntry.getSourceUri());
        enrichedElement.setItemSourcePath(itemDetail.sourcePath);
        enrichedElement.setItemOriginalFieldName(itemDetail.originalFieldName);
        enrichedElement.setItemModelHint(itemDetail.model);
        enrichedElement.setCleansedText(itemDetail.cleansedContent);
        enrichedElement.setEnrichedAt(OffsetDateTime.now());
        enrichedElement.setContext(objectMapper.convertValue(itemDetail.context, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}));

        @SuppressWarnings("unchecked")
        Map<String, Object> standardEnrichments = (Map<String, Object>) bedrockResponse.getOrDefault("standardEnrichments", bedrockResponse);

        enrichedElement.setSummary((String) standardEnrichments.get("summary"));
        enrichedElement.setSentiment((String) standardEnrichments.get("sentiment"));
        enrichedElement.setClassification((String) standardEnrichments.get("classification"));
        enrichedElement.setKeywords((List<String>) standardEnrichments.get("keywords"));
        enrichedElement.setTags((List<String>) standardEnrichments.get("tags"));
        enrichedElement.setBedrockModelUsed((String) bedrockResponse.get("enrichedWithModel"));
        enrichedElement.setStatus(elementStatus);
        // Create and set the enrichment metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("enrichedWithModel", bedrockResponse.get("enrichedWithModel"));
        metadata.put("enrichmentTimestamp", enrichedElement.getEnrichedAt().toString());
        try {
            enrichedElement.setEnrichmentMetadata(objectMapper.writeValueAsString(metadata));
        } catch (JsonProcessingException e) {
            logger.warn("Could not serialize enrichment metadata for item path: {}", itemDetail.sourcePath, e);
            enrichedElement.setEnrichmentMetadata("{\"error\":\"Could not serialize metadata\"}");
        }

        enrichedContentElementRepository.save(enrichedElement);
    }

    private void saveErrorEnrichedElement(CleansedItemDetail itemDetail, CleansedDataStore parentEntry, String status, String errorMessage) {
        EnrichedContentElement errorElement = new EnrichedContentElement();
        errorElement.setCleansedDataId(parentEntry.getId());
        errorElement.setVersion(parentEntry.getVersion());
        errorElement.setSourceUri(parentEntry.getSourceUri());
        errorElement.setItemSourcePath(itemDetail.sourcePath);
        errorElement.setItemOriginalFieldName(itemDetail.originalFieldName);
        errorElement.setItemModelHint(itemDetail.model);
        errorElement.setCleansedText(itemDetail.cleansedContent);
        errorElement.setEnrichedAt(OffsetDateTime.now());
        errorElement.setStatus(status);
        errorElement.setContext(objectMapper.convertValue(itemDetail.context, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}));

        Map<String, Object> bedrockMeta = new HashMap<>();
        bedrockMeta.put("enrichmentError", errorMessage);
        try {
            errorElement.setEnrichmentMetadata(objectMapper.writeValueAsString(bedrockMeta));
        } catch (JsonProcessingException e) {
            errorElement.setEnrichmentMetadata("Error could not serialize");
        }

        enrichedContentElementRepository.save(errorElement);
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