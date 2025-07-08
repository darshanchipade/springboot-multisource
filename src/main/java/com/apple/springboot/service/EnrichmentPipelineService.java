package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Service responsible for creating the enrichment pipeline.
 * It handles Bedrock enrichment, storing results in enriched_content_elements,
 * and then consolidates them into consolidated_enriched_sections for the data lake.
 */
@Service
public class EnrichmentPipelineService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentPipelineService.class);

    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final EnrichedContentElementRepository enrichedContentElementRepository;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;
    private final RateLimiter bedrockRateLimiter;

    private final ConsolidatedSectionService consolidatedSectionService;

    // Inner DTO for deserializing items from CleansedDataStore.cleansedItems
    // This should match the structure produced by DataIngestionService (List<Map<String, String>>)
    private static class CleansedItemDetail {
        public String sourcePath;       // from map key "sourcePath"
        public String originalFieldName;// from map key "originalFieldName"
        public String cleansedContent;  // from map key "cleansedContent"
        public String model;            // from map key "model" (modelHint)
    }

    public EnrichmentPipelineService(BedrockEnrichmentService bedrockEnrichmentService,
                                     EnrichedContentElementRepository enrichedContentElementRepository,
                                     CleansedDataStoreRepository cleansedDataStoreRepository,
                                     ObjectMapper objectMapper,
                                     RateLimiterRegistry rateLimiterRegistry,ConsolidatedSectionService consolidatedSectionService) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
        this.bedrockRateLimiter = rateLimiterRegistry.rateLimiter("bedrock");
        this.consolidatedSectionService = consolidatedSectionService;
    }

    /**
 * Entry point to trigger the enrichment process for a given CleansedDataStore record.
 * Applies text enrichment using AWS Bedrock for now, stores results in the database,
 * and invokes section-level consolidation which is for the conslidated data which will be further used for the search interface.
 *
 * @param cleansedDataEntry the CleansedDataStore entity to enrich
 * @throws JsonProcessingException if JSON processing fails
 */
    @Transactional
    public void enrichAndStore(CleansedDataStore cleansedDataEntry) throws JsonProcessingException {
        if (cleansedDataEntry == null || cleansedDataEntry.getId() == null) {
            logger.warn("Received null or no ID CleansedDataStore entry for enrichment. Skipping...");
            return;
        }

        UUID cleansedDataStoreId = cleansedDataEntry.getId();
        logger.info("Starting enrichment process for CleansedDataStore ID: {}", cleansedDataStoreId);

        String initialStatus = cleansedDataEntry.getStatus();
        if (!"CLEANSED_PENDING_ENRICHMENT".equals(initialStatus)) {
            logger.info("CleansedDataStore ID: {} is not in 'CLEANSED_PENDING_ENRICHMENT' state (current: {}). Skipping enrichment.",
                    cleansedDataStoreId, initialStatus);
            return;
        }

        cleansedDataEntry.setStatus("ENRICHMENT_IN_PROGRESS");
        cleansedDataStoreRepository.save(cleansedDataEntry); // Saving status change
       // consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);

        List<CleansedItemDetail> itemsToEnrich;
        List<Map<String, Object>> maps = cleansedDataEntry.getCleansedItems();
        if (maps == null || maps.isEmpty()) {
            logger.info("No items found in cleansed_items for CleansedDataStore ID: {}. Marking as ENRICHED_NO_ITEMS_TO_PROCESS.", cleansedDataStoreId);
            itemsToEnrich = Collections.emptyList();
        } else {
            itemsToEnrich = convertMapsToCleansedItemDetails(maps);
        }

        if (itemsToEnrich.isEmpty()) {
            logger.info("No actual items to process after deserialization/conversion for CleansedDataStore ID: {}. Marking as ENRICHED_NO_ITEMS_TO_PROCESS.", cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHED_NO_ITEMS_TO_PROCESS");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            return;
        }

        logger.info("Found {} items to enrich for CleansedDataStore ID: {}", itemsToEnrich.size(), cleansedDataStoreId);
        //For thread safe using Atomic instead of ++ counter
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger skippedByRateLimitCount = new AtomicInteger(0);
        List<String> itemProcessingErrors = new ArrayList<>();

        for (CleansedItemDetail itemDetail : itemsToEnrich) {
            if (itemDetail.cleansedContent == null || itemDetail.cleansedContent.trim().isEmpty()) {
                logger.warn("Skipping enrichment for item in CleansedDataStore ID: {} (path: {}) due to empty cleansed text.", cleansedDataStoreId, itemDetail.sourcePath);
                continue;
            }
            try {
                Map<String, Object> enrichmentResultsFromBedrock = bedrockRateLimiter.executeSupplier(
                        () -> bedrockEnrichmentService.enrichText(itemDetail.cleansedContent, itemDetail.model)
                );
                saveEnrichedElement(itemDetail, cleansedDataEntry, enrichmentResultsFromBedrock, "ENRICHED");
                successCount.incrementAndGet();
            } catch (RequestNotPermitted rnp) {
                logger.warn("Rate limit exceeded for Bedrock call (CleansedDataStore ID: {}, item path: {}). Item skipped.", cleansedDataStoreId, itemDetail.sourcePath);
                skippedByRateLimitCount.incrementAndGet();
                itemProcessingErrors.add(String.format("Item '%s': Rate limited - %s", itemDetail.sourcePath, rnp.getMessage()));
            } catch (Exception e) {
                logger.error("Error during enrichment for item (CleansedDataStore ID: {}, item path: {}): {}", cleansedDataStoreId, itemDetail.sourcePath, e.getMessage(), e);
                failureCount.incrementAndGet();
                itemProcessingErrors.add(String.format("Item '%s': Failed - %s", itemDetail.sourcePath, e.getMessage()));
            }
        }
       // consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);
        updateFinalCleansedDataStatus(cleansedDataEntry, successCount.get(), failureCount.get(), skippedByRateLimitCount.get(), itemsToEnrich.size(), itemProcessingErrors);
    }

    // Converts from the Map structure produced by DataIngestionService to CleansedItemDetail DTO
    /**
 * Converts a list of raw map entries from cleansed_items into structured CleansedItemDetail DTOs.
 *
 * @param maps list of raw maps from CleansedDataStore.cleansedItems
 * @return list of structured CleansedItemDetail objects
 */
    private List<CleansedItemDetail> convertMapsToCleansedItemDetails(List<Map<String, Object>> maps) {
        if (maps == null) return Collections.emptyList();
        List<CleansedItemDetail> details = new ArrayList<>();
        for (Map<String, Object> map : maps) {
            CleansedItemDetail detail = new CleansedItemDetail();
            detail.sourcePath = map.get("sourcePath") != null ? map.get("sourcePath").toString() : null;
            detail.originalFieldName = map.get("originalFieldName") != null ? map.get("originalFieldName").toString() : null;
            detail.cleansedContent = map.get("cleansedContent") != null ? map.get("cleansedContent").toString() : null;
            detail.model = map.get("model") != null ? map.get("model").toString() : null;

            if (detail.sourcePath != null && detail.originalFieldName != null && detail.cleansedContent != null) {
                details.add(detail);
            } else {
                logger.warn("Skipping map in convertMapsToCleansedItemDetails due to missing essential fields (sourcePath, originalFieldName, or cleansedContent): {}", map);
            }
        }
        return details;
    }
    
    
    /**
 * Persists one enriched content element into enriched_content_elements table.
 * Extracts structured fields like keywords, tags, and stores enrichment metadata.
 *
 * @param itemDetail cleansed content item being enriched
 * @param parentEntry parent CleansedDataStore record
 * @param enrichmentResults enrichment results returned from Bedrock
 * @param elementStatus status to assign to this enriched element (e.g "ENRICHED")
 */
    private void saveEnrichedElement(CleansedItemDetail itemDetail, CleansedDataStore parentEntry,
                                     Map<String, Object> enrichmentResults, String elementStatus) {
        EnrichedContentElement enrichedElement = new EnrichedContentElement();
        enrichedElement.setCleansedDataId(parentEntry.getId());
        enrichedElement.setSourceUri(parentEntry.getSourceUri());
        enrichedElement.setItemSourcePath(itemDetail.sourcePath);
        enrichedElement.setItemOriginalFieldName(itemDetail.originalFieldName);
        enrichedElement.setItemModelHint(itemDetail.model);
        enrichedElement.setCleansedText(itemDetail.cleansedContent);
        enrichedElement.setEnrichedAt(OffsetDateTime.now());

        enrichedElement.setSummary((String) enrichmentResults.getOrDefault("summary", "Error: Missing summary"));

        Object keywordsObj = enrichmentResults.get("keywords");
        if (keywordsObj instanceof List) {
            try {
                @SuppressWarnings("unchecked")
                List<String> keywordsList = (List<String>) keywordsObj;
                enrichedElement.setKeywords(keywordsList.toArray(new String[0]));
            } catch (ClassCastException e) {
                logger.warn("Could not cast keywords to List<String> for item path: {}. Keywords: {}", itemDetail.sourcePath, keywordsObj, e);
                enrichedElement.setKeywords(new String[]{"Error: Keywords format unexpected"});
            }
        } else {
            logger.warn("Keywords field was not a List for item path: {}. Received: {}", itemDetail.sourcePath, keywordsObj != null ? keywordsObj.getClass().getName() : "null");
            enrichedElement.setKeywords(new String[0]);
        }

        enrichedElement.setSentiment((String) enrichmentResults.getOrDefault("sentiment", "Error: Missing sentiment"));
        enrichedElement.setClassification((String) enrichmentResults.getOrDefault("classification", "Error: Missing classification"));

        Object tagsObj = enrichmentResults.get("tags");
        if (tagsObj instanceof List) {
            try {
                @SuppressWarnings("unchecked")
                List<String> tagsList = (List<String>) tagsObj;
                enrichedElement.setTags(tagsList.toArray(new String[0]));
            } catch (ClassCastException e) {
                logger.warn("Could not cast tags to List<String> for item path: {}. Tags: {}", itemDetail.sourcePath, tagsObj, e);
                enrichedElement.setTags(new String[]{"Error: Tags format unexpected"});
            }
        } else {
            logger.warn("Tags field was not a List for item path: {}. Received: {}", itemDetail.sourcePath, tagsObj != null ? tagsObj.getClass().getName() : "null");
            enrichedElement.setTags(new String[0]);
        }

        enrichedElement.setBedrockModelUsed((String) enrichmentResults.getOrDefault("enrichedWithModel", bedrockEnrichmentService.getConfiguredModelId()));

        Map<String, Object> bedrockMeta = new HashMap<>(enrichmentResults);
        bedrockMeta.remove("summary");
        bedrockMeta.remove("keywords");
        bedrockMeta.remove("sentiment");
        bedrockMeta.remove("classification");
        bedrockMeta.remove("tags");
        bedrockMeta.remove("enrichedWithModel");
        if (enrichmentResults.containsKey("error")) {
            bedrockMeta.put("bedrockItemProcessingError", enrichmentResults.get("error"));
        }

        if (!bedrockMeta.isEmpty()) {
            try {
                enrichedElement.setEnrichmentMetadata(objectMapper.writeValueAsString(bedrockMeta));
            } catch (JsonProcessingException e) {
                logger.error("Error serializing Bedrock metadata for item path {}: {}", itemDetail.sourcePath, e.getMessage());
                enrichedElement.setEnrichmentMetadata("{\"error\":\"Could not serialize Bedrock metadata\"}");
            }
        }
        enrichedElement.setStatus(elementStatus);
        enrichedContentElementRepository.save(enrichedElement);
    }
    
    
    /**
 * Finalizes the enrichment process by determining the final status of the CleansedDataStore record,
 * generating a summary of enrichment and storing it in the cleansingErrors field.
 *
 * @param cleansedDataEntry the CleansedDataStore record to update
 * @param successCount number of items successfully enriched
 * @param failureCount number of items failed during enrichment
 * @param skippedByRateLimitCount number of items skipped due to rate limiting
 * @param totalDeserializedItems total number of items attempted
 * @param itemProcessingErrors list of error messages during enrichment which will help us determine the excat issue
 * @throws JsonProcessingException if serializing error summary fails
 */

    private void updateFinalCleansedDataStatus(CleansedDataStore cleansedDataEntry, int successCount, int failureCount, int skippedByRateLimitCount, int totalDeserializedItems, List<String> itemProcessingErrors) throws JsonProcessingException {
        String finalStatus;
        int itemsAttempted = successCount + failureCount + skippedByRateLimitCount;

        if (totalDeserializedItems == 0) {
            finalStatus = "ENRICHED_NO_ITEMS_TO_PROCESS";
        } else if (itemsAttempted == 0 && totalDeserializedItems > 0) {
            finalStatus = "ENRICHED_ALL_SKIPPED_EMPTY_TEXT";
        } else if (failureCount == 0 && skippedByRateLimitCount == 0 && successCount > 0 && successCount == itemsAttempted) {
            finalStatus = "ENRICHED_COMPLETE";
        } else if (successCount > 0) {
            finalStatus = "PARTIALLY_ENRICHED";
        } else if (failureCount > 0 && successCount == 0 && skippedByRateLimitCount == 0) {
            finalStatus = "ENRICHMENT_FAILED_ALL_ATTEMPTED";
        } else if (skippedByRateLimitCount > 0 && successCount == 0 && failureCount == 0) {
            finalStatus = "ENRICHMENT_SKIPPED_ALL_RATE_LIMIT";
        } else {
            finalStatus = "ENRICHMENT_ISSUES_DETECTED";
        }

        cleansedDataEntry.setStatus(finalStatus);
        Map<String, Object> processingSummary = new HashMap<>();
        processingSummary.put("totalDeserializedItems", totalDeserializedItems);
        processingSummary.put("itemsAttemptedForEnrichment", itemsAttempted);
        processingSummary.put("successfullyEnriched", successCount);
        processingSummary.put("failedEnrichmentAttempts", failureCount);
        processingSummary.put("skippedByRateLimit", skippedByRateLimitCount);
        if (!itemProcessingErrors.isEmpty()) {
            try {
                List<String> shortErrorMessages = itemProcessingErrors.stream()
                        .map(err -> err.length() > 255 ? err.substring(0, 252) + "..." : err)
                        .collect(Collectors.toList());
                processingSummary.put("itemProcessingErrorMessages", shortErrorMessages);
            } catch (Exception e) {
                logger.error("Error processing itemProcessingErrorMessages for summary: {}", e.getMessage());
                processingSummary.put("itemProcessingErrorMessages", "Error list too large or unprocessable");
            }
        }
        updateEnrichmentErrorDetails(cleansedDataEntry, "EnrichmentRunSummary", processingSummary);
        cleansedDataStoreRepository.save(cleansedDataEntry);
        logger.info("Finished enrichment for CleansedDataStore ID: {}. Final status: {}. Summary: {}", cleansedDataEntry.getId(), finalStatus, processingSummary);
    }
    
    /**
 * Adds an enrichment-related error or summary to the cleansingErrors field of the CleansedDataStore record.
 *
 * @param cleansedDataEntry the CleansedDataStore to update
 * @param errorKey a descriptive label for the error (e.g., "EnrichmentRunSummary")
 * @param errorValue the error content, can be a string or map
 * @throws JsonProcessingException if serialization of errorValue fails
 */

    private void updateEnrichmentErrorDetails(CleansedDataStore cleansedDataEntry, String errorKey, Object errorValue) throws JsonProcessingException {
        Map<String, Object> errorsMap;

        if (cleansedDataEntry.getCleansingErrors() != null) {
            try {
                errorsMap = cleansedDataEntry.getCleansingErrors();
            } catch (Exception e) {
                logger.warn("Failed to use existing cleansing_errors map, initializing new one. Error: {}", e.getMessage());
                errorsMap = new HashMap<>();
                errorsMap.put("parsing_error_for_existing_cleansing_errors", cleansedDataEntry.getCleansingErrors().toString());
            }
        } else {
            errorsMap = new HashMap<>();
        }

        errorsMap.put(OffsetDateTime.now().toString() + "_" + errorKey, errorValue);
        cleansedDataEntry.setCleansingErrors(errorsMap);
    }
}