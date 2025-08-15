package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.repository.CleansedDataStoreRepository;
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

    private static class CleansedItemDetail {
        public final String sourcePath;
        public final String originalFieldName;
        public final String cleansedContent;
        public final String model;
        public final String contentHash;

        public CleansedItemDetail(String sourcePath, String originalFieldName, String cleansedContent, String model, String contentHash) {
            this.sourcePath = sourcePath;
            this.originalFieldName = originalFieldName;
            this.cleansedContent = cleansedContent;
            this.model = model;
            this.contentHash = contentHash;
        }
    }

    public EnrichmentPipelineService(BedrockEnrichmentService bedrockEnrichmentService,
                                     EnrichedContentElementRepository enrichedContentElementRepository,
                                     CleansedDataStoreRepository cleansedDataStoreRepository,
                                     ObjectMapper objectMapper,
                                     ConsolidatedSectionService consolidatedSectionService,
                                     AIResponseValidator aiResponseValidator) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
        this.consolidatedSectionService = consolidatedSectionService;
        this.aiResponseValidator = aiResponseValidator;
    }

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
        cleansedDataStoreRepository.save(cleansedDataEntry);

        List<Map<String, Object>> maps = cleansedDataEntry.getCleansedItems();
        if (maps == null || maps.isEmpty()) {
            logger.info("No items found in cleansed_items for CleansedDataStore ID: {}. Marking as ENRICHED_NO_ITEMS_TO_PROCESS.", cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHED_NO_ITEMS_TO_PROCESS");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            return;
        }

        List<CleansedItemDetail> itemsToEnrich = convertMapsToCleansedItemDetails(maps);

        logger.info("Found {} items to enrich for CleansedDataStore ID: {}", itemsToEnrich.size(), cleansedDataStoreId);
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
                JsonNode itemAsJson = objectMapper.valueToTree(itemDetail);
                Map<String, Object> enrichmentResultsFromBedrock = bedrockEnrichmentService.enrichItem(itemAsJson);

                if (enrichmentResultsFromBedrock.containsKey("error")) {
                    throw new RuntimeException("Bedrock enrichment failed: " + enrichmentResultsFromBedrock.get("error"));
                }

                if (!aiResponseValidator.isValid(enrichmentResultsFromBedrock)) {
                    throw new RuntimeException("Validation failed for AI response structure. Check logs for details.");
                }

                saveEnrichedElement(itemDetail, cleansedDataEntry, enrichmentResultsFromBedrock, "ENRICHED");
                successCount.incrementAndGet();
            } catch (RequestNotPermitted rnp) {
                logger.warn("Rate limit exceeded for Bedrock call (CleansedDataStore ID: {}, item path: {}). Item skipped.", cleansedDataStoreId, itemDetail.sourcePath, rnp);
                skippedByRateLimitCount.incrementAndGet();
                itemProcessingErrors.add(String.format("Item '%s': Rate limited - %s", itemDetail.sourcePath, rnp.getMessage()));
            } catch (Exception e) {
                logger.error("Error during enrichment for item (CleansedDataStore ID: {}, item path: {}): {}", cleansedDataStoreId, itemDetail.sourcePath, e.getMessage(), e);
                failureCount.incrementAndGet();
                itemProcessingErrors.add(String.format("Item '%s': Failed - %s", itemDetail.sourcePath, e.getMessage()));
            }
        }
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);
        updateFinalCleansedDataStatus(cleansedDataEntry, successCount.get(), failureCount.get(), skippedByRateLimitCount.get(), itemsToEnrich.size(), itemProcessingErrors);
    }

    private List<CleansedItemDetail> convertMapsToCleansedItemDetails(List<Map<String, Object>> maps) {
        return maps.stream()
                .map(map -> {
                    String sourcePath = map.get("sourcePath") != null ? map.get("sourcePath").toString() : null;
                    String originalFieldName = map.get("originalFieldName") != null ? map.get("originalFieldName").toString() : null;
                    String cleansedContent = map.get("cleansedContent") != null ? map.get("cleansedContent").toString() : null;
                    String model = map.get("model") != null ? map.get("model").toString() : null;
                    String contentHash = map.get("contentHash") != null ? map.get("contentHash").toString() : null;

                    if (sourcePath != null && originalFieldName != null && cleansedContent != null) {
                        return new CleansedItemDetail(sourcePath, originalFieldName, cleansedContent, model, contentHash);
                    } else {
                        logger.warn("Skipping map in convertMapsToCleansedItemDetails due to missing essential fields: {}", map);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private void saveEnrichedElement(CleansedItemDetail itemDetail, CleansedDataStore parentEntry,
                                     Map<String, Object> bedrockResponse, String elementStatus) {
        EnrichedContentElement enrichedElement = new EnrichedContentElement();
        enrichedElement.setCleansedDataId(parentEntry.getId());
        enrichedElement.setVersion(parentEntry.getVersion());
        enrichedElement.setSourceUri(parentEntry.getSourceUri());
        enrichedElement.setItemSourcePath(itemDetail.sourcePath);
        enrichedElement.setItemOriginalFieldName(itemDetail.originalFieldName);
        enrichedElement.setItemModelHint(itemDetail.model);
        enrichedElement.setCleansedText(itemDetail.cleansedContent);
        enrichedElement.setEnrichedAt(OffsetDateTime.now());

        // Extract the nested objects from the Bedrock response
        @SuppressWarnings("unchecked")
        Map<String, Object> standardEnrichments = (Map<String, Object>) bedrockResponse.getOrDefault("standardEnrichments", Collections.emptyMap());
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) bedrockResponse.getOrDefault("context", Collections.emptyMap());

        enrichedElement.setContext(context);

        enrichedElement.setSummary((String) standardEnrichments.getOrDefault("summary", "Error: Missing summary"));
        enrichedElement.setSentiment((String) standardEnrichments.getOrDefault("sentiment", "Error: Missing sentiment"));
        enrichedElement.setClassification((String) standardEnrichments.getOrDefault("classification", "Error: Missing classification"));

        enrichedElement.setKeywords(extractList(standardEnrichments, "keywords", itemDetail.sourcePath).toArray(new String[0]));
        enrichedElement.setTags(extractList(standardEnrichments, "tags", itemDetail.sourcePath).toArray(new String[0]));

        // Provenance is now inside the context map
        @SuppressWarnings("unchecked")
        Map<String, Object> provenance = (Map<String, Object>) context.getOrDefault("provenance", Collections.emptyMap());
        enrichedElement.setBedrockModelUsed((String) provenance.getOrDefault("modelId", bedrockEnrichmentService.getConfiguredModelId()));

        // Store any other non-standard fields in the metadata column
        Map<String, Object> bedrockMeta = new HashMap<>(bedrockResponse);
        bedrockMeta.remove("standardEnrichments");
        bedrockMeta.remove("context");
        if (bedrockResponse.containsKey("error")) {
            bedrockMeta.put("bedrockItemProcessingError", bedrockResponse.get("error"));
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

    private List<String> extractList(Map<String, Object> map, String key, String sourcePath) {
        Object obj = map.get(key);
        if (obj instanceof List) {
            try {
                @SuppressWarnings("unchecked")
                List<String> list = (List<String>) obj;
                return list;
            } catch (ClassCastException e) {
                logger.warn("Could not cast {} to List<String> for item path: {}. Value: {}", key, sourcePath, obj, e);
            }
        }
        logger.warn("{} field was not a List for item path: {}. Received: {}", key, sourcePath, obj != null ? obj.getClass().getName() : "null");
        return Collections.emptyList();
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