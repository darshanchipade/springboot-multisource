package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.service.CleansedItemDetail;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class EnrichmentPersistenceService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentPersistenceService.class);

    private final EnrichedContentElementRepository enrichedContentElementRepository;
    private final ObjectMapper objectMapper;

    public EnrichmentPersistenceService(EnrichedContentElementRepository enrichedContentElementRepository, ObjectMapper objectMapper) {
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void saveEnrichedElement(CleansedItemDetail itemDetail, CleansedDataStore parentEntry,
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

    @Transactional
    public void saveErrorEnrichedElement(CleansedItemDetail itemDetail, CleansedDataStore parentEntry, String status, String errorMessage) {
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
        logger.debug("Saved error element for item path: {}", itemDetail.sourcePath);
    }
}