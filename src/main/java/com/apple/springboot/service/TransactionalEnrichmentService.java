package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TransactionalEnrichmentService {

    private static final Logger logger = LoggerFactory.getLogger(TransactionalEnrichmentService.class);
    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final EnrichedContentElementRepository enrichedContentElementRepository;
    private final ObjectMapper objectMapper;
    private final AIResponseValidator aiResponseValidator;
    private final JobProgressService jobProgressService;

    public TransactionalEnrichmentService(BedrockEnrichmentService bedrockEnrichmentService,
                                          EnrichedContentElementRepository enrichedContentElementRepository,
                                          ObjectMapper objectMapper,
                                          AIResponseValidator aiResponseValidator,
                                          JobProgressService jobProgressService) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.objectMapper = objectMapper;
        this.aiResponseValidator = aiResponseValidator;
        this.jobProgressService = jobProgressService;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Retry(name = "bedrockRetry")
    public EnrichmentResult enrichBatch(List<CleansedItemDetail> batch, CleansedDataStore cleansedDataEntry, String jobId) {
        try {
            logger.info("Processing batch of {} items for CleansedDataStore ID: {}", batch.size(), cleansedDataEntry.getId());
            jobProgressService.updateProgress(jobId, "Attempting to process batch of " + batch.size() + " items.");

            Map<String, Map<String, Object>> batchResults = bedrockEnrichmentService.enrichBatch(batch);
            logger.debug("Batch results: {}", batchResults);

            List<EnrichedContentElement> successfulElements = new ArrayList<>();

            for (CleansedItemDetail item : batch) {
                String fullContextId = item.sourcePath + "::" + item.originalFieldName;
                Map<String, Object> result = batchResults.getOrDefault(fullContextId, new HashMap<>());

                if (result.containsKey("error")) {
                    String errorMsg = (String) result.get("error");
                    logger.warn("Batch item failed for CleansedDataStore ID: {}, path: {}: {}", cleansedDataEntry.getId(), item.sourcePath, errorMsg);
                    saveErrorEnrichedElement(item, cleansedDataEntry, "ERROR_ENRICHMENT_FAILED", errorMsg);
                    jobProgressService.updateProgress(jobId, "FAILED (in batch): " + item.sourcePath + " - " + errorMsg);
                    continue;
                }
                if (!aiResponseValidator.isValid(result)) {
                    logger.warn("Validation failed for batch item response, path: {}", item.sourcePath);
                    saveErrorEnrichedElement(item, cleansedDataEntry, "ERROR_ENRICHMENT_FAILED", "Invalid AI response structure");
                    jobProgressService.updateProgress(jobId, "FAILED (in batch): " + item.sourcePath + " - Invalid AI response");
                    continue;
                }

                Map<String, Object> contextMap = objectMapper.convertValue(item.context, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
                contextMap.put("fullContextId", fullContextId);
                contextMap.put("sourcePath", item.sourcePath);
                Map<String, Object> provenance = new HashMap<>();
                provenance.put("modelId", bedrockEnrichmentService.getConfiguredModelId());
                contextMap.put("provenance", provenance);
                result.put("context", contextMap);

                EnrichedContentElement savedElement = saveEnrichedElement(item, cleansedDataEntry, result, "ENRICHED");
                successfulElements.add(savedElement);
            }
            logger.info("Batch processed: successCount={}, totalItems={}", successfulElements.size(), batch.size());
            jobProgressService.updateProgress(jobId, "Batch finished. " + successfulElements.size() + "/" + batch.size() + " successful.");
            return EnrichmentResult.success(successfulElements);
        } catch (Exception e) {
            // This is a catastrophic failure for the whole batch. The caller will handle the fallback.
            logger.error("Catastrophic error during batch enrichment for CleansedDataStore ID: {}: {}", cleansedDataEntry.getId(), e.getMessage(), e);
            jobProgressService.updateProgress(jobId, "Batch of " + batch.size() + " items failed entirely. Switching to single-item mode. Error: " + e.getMessage());
            // Re-throw the exception to trigger the fallback logic in the pipeline service.
            throw new RuntimeException("Batch enrichment failed, triggering fallback.", e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Retry(name = "bedrockRetry")
    public EnrichmentResult enrichItem(CleansedItemDetail itemDetail, CleansedDataStore cleansedDataEntry, String jobId) {
        jobProgressService.updateProgress(jobId, "Starting single-item enrichment for: " + itemDetail.sourcePath);
        try {
            if (itemDetail.cleansedContent == null || itemDetail.cleansedContent.trim().isEmpty()) {
                logger.warn("Skipping enrichment for item in CleansedDataStore ID: {} (path: {}) due to empty cleansed text.", cleansedDataEntry.getId(), itemDetail.sourcePath);
                jobProgressService.updateProgress(jobId, "SKIPPED (empty content): " + itemDetail.sourcePath);
                return EnrichmentResult.skipped();
            }

            Map<String, String> itemContent = new HashMap<>();
            itemContent.put("cleansedContent", itemDetail.cleansedContent);
            JsonNode itemContentAsJson = objectMapper.valueToTree(itemContent);

            Map<String, Object> enrichmentResultsFromBedrock = bedrockEnrichmentService.enrichItem(itemContentAsJson, itemDetail.context);

            if (enrichmentResultsFromBedrock.containsKey("error")) {
                throw new RuntimeException("Bedrock enrichment failed: " + enrichmentResultsFromBedrock.get("error"));
            }

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

            EnrichedContentElement savedElement = saveEnrichedElement(itemDetail, cleansedDataEntry, enrichmentResultsFromBedrock, "ENRICHED");
            jobProgressService.updateProgress(jobId, "SUCCESS: " + itemDetail.sourcePath);
            return EnrichmentResult.success(savedElement);
        } catch (RequestNotPermitted rnp) {
            logger.warn("Rate limit exceeded for Bedrock call (CleansedDataStore ID: {}, item path: {}). Item skipped.", cleansedDataEntry.getId(), itemDetail.sourcePath, rnp);
            saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_RATE_LIMITED", rnp.getMessage());
            jobProgressService.updateProgress(jobId, "RATE LIMITED: " + itemDetail.sourcePath);
            return EnrichmentResult.rateLimited(String.format("Item '%s': Rate limited - %s", itemDetail.sourcePath, rnp.getMessage()));
        } catch (Exception e) {
            logger.error("Error during enrichment for item (CleansedDataStore ID: {}, item path: {}): {}", cleansedDataEntry.getId(), itemDetail.sourcePath, e.getMessage(), e);
            saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_ENRICHMENT_FAILED", e.getMessage());
            jobProgressService.updateProgress(jobId, "FAILED: " + itemDetail.sourcePath + " - " + e.getMessage());
            return EnrichmentResult.failure(String.format("Item '%s': Failed - %s", itemDetail.sourcePath, e.getMessage()));
        }
    }

    private EnrichedContentElement saveEnrichedElement(CleansedItemDetail itemDetail, CleansedDataStore parentEntry,
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
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("enrichedWithModel", bedrockResponse.get("enrichedWithModel"));
        metadata.put("enrichmentTimestamp", enrichedElement.getEnrichedAt().toString());
        try {
            enrichedElement.setEnrichmentMetadata(objectMapper.writeValueAsString(metadata));
        } catch (JsonProcessingException e) {
            logger.warn("Could not serialize enrichment metadata for item path: {}", itemDetail.sourcePath, e);
            enrichedElement.setEnrichmentMetadata("{\"error\":\"Could not serialize metadata\"}");
        }

        return enrichedContentElementRepository.save(enrichedElement);
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
}