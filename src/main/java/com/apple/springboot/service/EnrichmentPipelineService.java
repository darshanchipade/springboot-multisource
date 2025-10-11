package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.service.CleansedItemDetail;
import com.apple.springboot.model.EnrichmentContext;
import com.apple.springboot.model.EnrichmentMessage;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class EnrichmentPipelineService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentPipelineService.class);

    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;
    private final SqsService sqsService;

    public EnrichmentPipelineService(CleansedDataStoreRepository cleansedDataStoreRepository,
                                     ObjectMapper objectMapper,
                                     SqsService sqsService) {
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
        this.sqsService = sqsService;
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

        // Deduplicate by (sourcePath, originalFieldName) to avoid re-queueing identical items
        Map<String, CleansedItemDetail> uniqueByKey = new LinkedHashMap<>();
        for (CleansedItemDetail d : itemsToEnrich) {
            if (d == null) continue;
            String key = (d.sourcePath == null ? "" : d.sourcePath) + "||" + (d.originalFieldName == null ? "" : d.originalFieldName);
            uniqueByKey.putIfAbsent(key, d);
        }
        List<CleansedItemDetail> uniqueItems = new java.util.ArrayList<>(uniqueByKey.values());

        for (CleansedItemDetail itemDetail : uniqueItems) {
            if (itemDetail.cleansedContent == null || itemDetail.cleansedContent.trim().isEmpty()) {
                logger.warn("Skipping enrichment for item in CleansedDataStore ID: {} (path: {}) due to empty cleansed text.", cleansedDataStoreId, itemDetail.sourcePath);
                continue;
            }
            EnrichmentMessage message = new EnrichmentMessage(itemDetail, cleansedDataStoreId);
            sqsService.sendMessage(message);
        }

        cleansedDataEntry.setStatus("ENRICHMENT_QUEUED");
        cleansedDataStoreRepository.save(cleansedDataEntry);
        logger.info("Queued {} unique items (from {} total) for CleansedDataStore ID: {}. Status: ENRICHMENT_QUEUED",
                uniqueItems.size(), itemsToEnrich.size(), cleansedDataEntry.getId());
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
}