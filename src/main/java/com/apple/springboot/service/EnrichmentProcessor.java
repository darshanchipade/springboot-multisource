package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class EnrichmentProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentProcessor.class);

    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final EnrichedContentElementRepository enrichedContentElementRepository;
    private final ConsolidatedSectionService consolidatedSectionService;
    private final TextChunkingService textChunkingService;
    private final ContentChunkRepository contentChunkRepository;
    private final RateLimiter bedrockChatRateLimiter;
    private final RateLimiter bedrockEmbedRateLimiter;
    private final EnrichmentPersistenceService persistenceService;
    private final AIResponseValidator aiResponseValidator;
    private final ObjectMapper objectMapper;


    @SuppressWarnings("UnstableApiUsage")
    public EnrichmentProcessor(BedrockEnrichmentService bedrockEnrichmentService,
                               CleansedDataStoreRepository cleansedDataStoreRepository,
                               EnrichedContentElementRepository enrichedContentElementRepository,
                               ConsolidatedSectionService consolidatedSectionService,
                               TextChunkingService textChunkingService,
                               ContentChunkRepository contentChunkRepository,
                               RateLimiter bedrockChatRateLimiter,
                               RateLimiter bedrockEmbedRateLimiter,
                               EnrichmentPersistenceService persistenceService,
                               AIResponseValidator aiResponseValidator,
                               ObjectMapper objectMapper) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.consolidatedSectionService = consolidatedSectionService;
        this.textChunkingService = textChunkingService;
        this.contentChunkRepository = contentChunkRepository;
        this.bedrockChatRateLimiter = bedrockChatRateLimiter;
        this.bedrockEmbedRateLimiter = bedrockEmbedRateLimiter;
        this.persistenceService = persistenceService;
        this.aiResponseValidator = aiResponseValidator;
        this.objectMapper = objectMapper;
    }

    // This method is NO LONGER @Transactional
    public void process(EnrichmentMessage message) {
        // Bedrock rate limiting is enforced inside BedrockEnrichmentService per operation

        CleansedItemDetail itemDetail = message.getCleansedItemDetail();
        UUID cleansedDataStoreId = message.getCleansedDataStoreId();

        // We need to fetch the entry outside of the main try-catch
        CleansedDataStore cleansedDataEntry = cleansedDataStoreRepository.findById(cleansedDataStoreId)
                .orElse(null);

        if (cleansedDataEntry == null) {
            logger.error("Could not find CleansedDataStore with ID: {}. Cannot process item.", cleansedDataStoreId);
            return;
        }

        try {
            Map<String, String> itemContent = new HashMap<>();
            itemContent.put("cleansedContent", itemDetail.cleansedContent);
            JsonNode itemContentAsJson = objectMapper.valueToTree(itemContent);

            Map<String, Object> enrichmentResultsFromBedrock = bedrockEnrichmentService.enrichItem(itemContentAsJson, itemDetail.context);

            if (enrichmentResultsFromBedrock.containsKey("error")) {
                String errorMessage = "Bedrock enrichment failed: " + enrichmentResultsFromBedrock.get("error");
                logger.error(errorMessage);
                persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_ENRICHMENT_FAILED", errorMessage);
            } else {
                Map<String, Object> contextMap = objectMapper.convertValue(itemDetail.context, new com.fasterxml.jackson.core.type.TypeReference<>() {});
                String fullContextId = itemDetail.sourcePath + "::" + itemDetail.originalFieldName;
                contextMap.put("fullContextId", fullContextId);
                contextMap.put("sourcePath", itemDetail.sourcePath);
                Map<String, Object> provenance = new HashMap<>();
                provenance.put("modelId", bedrockEnrichmentService.getConfiguredModelId());
                contextMap.put("provenance", provenance);
                enrichmentResultsFromBedrock.put("context", contextMap);

                if (!aiResponseValidator.isValid(enrichmentResultsFromBedrock)) {
                    String validationError = "Validation failed for AI response structure. Check logs for details: " + objectMapper.writeValueAsString(enrichmentResultsFromBedrock);
                    logger.error(validationError);
                    persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_VALIDATION_FAILED", validationError);
                } else {
                    persistenceService.saveEnrichedElement(itemDetail, cleansedDataEntry, enrichmentResultsFromBedrock, "ENRICHED");
                }
            }
        } catch (ThrottledException te) {
            // Do not delete SQS message; allow visibility timeout to expire so it retries later
            logger.warn("Bedrock throttling for item (CleansedDataStore ID: {}, item path: {}). Will retry via SQS.", cleansedDataStoreId, itemDetail.sourcePath);
            // Re-throw to inform caller not to delete SQS message
            throw te;
        } catch (Exception e) {
            logger.error("Critical error during enrichment for item (CleansedDataStore ID: {}, item path: {}): {}", cleansedDataStoreId, itemDetail.sourcePath, e.getMessage(), e);
            persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_UNEXPECTED", e.getMessage());
        } finally {
            // This 'finally' block ensures that we always check for completion.
            checkCompletion(cleansedDataEntry);
        }
    }

    private void checkCompletion(CleansedDataStore cleansedDataEntry) {
        long totalItems = cleansedDataEntry.getCleansedItems().size();
        long processedCount = enrichedContentElementRepository.countByCleansedDataId(cleansedDataEntry.getId());

        logger.trace("Completion check for {}: {}/{} items processed.", cleansedDataEntry.getId(), processedCount, totalItems);

        if (processedCount >= totalItems) {
            logger.info("All items for CleansedDataStore ID {} have been processed. Running finalization steps.", cleansedDataEntry.getId());
            runFinalizationSteps(cleansedDataEntry);
        }
    }

    @Transactional
    public void runFinalizationSteps(CleansedDataStore cleansedDataEntry) {
        logger.info("Running finalization steps for CleansedDataStore ID: {}", cleansedDataEntry.getId());
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);

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
                }
            }
        }
        updateFinalCleansedDataStatus(cleansedDataEntry);
    }

    private void updateFinalCleansedDataStatus(CleansedDataStore cleansedDataEntry) {
        long errorCount = enrichedContentElementRepository.countByCleansedDataIdAndStatusContaining(cleansedDataEntry.getId(), "ERROR");
        long successCount = enrichedContentElementRepository.countByCleansedDataIdAndStatus(cleansedDataEntry.getId(), "ENRICHED");

        String finalStatus;
        if (errorCount == 0 && successCount == cleansedDataEntry.getCleansedItems().size()) {
            finalStatus = "ENRICHED_COMPLETE";
        } else if (successCount > 0 || errorCount > 0) {
            finalStatus = "PARTIALLY_ENRICHED";
        } else {
            finalStatus = "ENRICHMENT_FAILED";
        }
        cleansedDataEntry.setStatus(finalStatus);
        cleansedDataStoreRepository.save(cleansedDataEntry);
        logger.info("Finished enrichment for CleansedDataStore ID: {}. Final status: {}", cleansedDataEntry.getId(), finalStatus);
    }
}