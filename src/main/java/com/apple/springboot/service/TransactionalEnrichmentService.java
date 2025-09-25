package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.EnrichedContentElement;
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
import com.apple.springboot.repository.EnrichedContentElementRepository;

import java.time.OffsetDateTime;
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

    public TransactionalEnrichmentService(BedrockEnrichmentService bedrockEnrichmentService,
                                          EnrichedContentElementRepository enrichedContentElementRepository,
                                          ObjectMapper objectMapper,
                                          AIResponseValidator aiResponseValidator) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.objectMapper = objectMapper;
        this.aiResponseValidator = aiResponseValidator;
    }

    //@Transactional(propagation = Propagation.REQUIRES_NEW)
    @Retry(name = "bedrockRetry")
    public EnrichmentResult enrichItem(CleansedItemDetail itemDetail, CleansedDataStore cleansedDataEntry) {
        try {
            if (itemDetail.cleansedContent == null || itemDetail.cleansedContent.trim().isEmpty()) {
                logger.warn("Skipping enrichment for item in CleansedDataStore ID: {} (path: {}) due to empty cleansed text.", cleansedDataEntry.getId(), itemDetail.sourcePath);
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

            saveEnrichedElement(itemDetail, cleansedDataEntry, enrichmentResultsFromBedrock, "ENRICHED");
            return EnrichmentResult.success();
        } catch (RequestNotPermitted rnp) {
            logger.warn("Rate limit exceeded for Bedrock call (CleansedDataStore ID: {}, item path: {}). Item skipped.", cleansedDataEntry.getId(), itemDetail.sourcePath, rnp);
            saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_RATE_LIMITED", rnp.getMessage());
            return EnrichmentResult.rateLimited(String.format("Item '%s': Rate limited - %s", itemDetail.sourcePath, rnp.getMessage()));
        } catch (Exception e) {
            logger.error("Error during enrichment for item (CleansedDataStore ID: {}, item path: {}): {}", cleansedDataEntry.getId(), itemDetail.sourcePath, e.getMessage(), e);
            saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_ENRICHMENT_FAILED", e.getMessage());
            return EnrichmentResult.failure(String.format("Item '%s': Failed - %s", itemDetail.sourcePath, e.getMessage()));
        }
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
}
