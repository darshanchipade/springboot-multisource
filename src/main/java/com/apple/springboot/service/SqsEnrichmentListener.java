package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.CleansedItemDetail;
import com.apple.springboot.model.EnrichmentContext;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.service.EnrichmentPipelineService;
import com.apple.springboot.service.TransactionalEnrichmentService;
import com.apple.springboot.model.MessagePayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class SqsEnrichmentListener {

    private static final Logger logger = LoggerFactory.getLogger(SqsEnrichmentListener.class);

    private final TransactionalEnrichmentService transactionalEnrichmentService;
    private final EnrichmentPipelineService enrichmentPipelineService;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;

    public SqsEnrichmentListener(TransactionalEnrichmentService transactionalEnrichmentService,
                                 EnrichmentPipelineService enrichmentPipelineService,
                                 CleansedDataStoreRepository cleansedDataStoreRepository,
                                 ObjectMapper objectMapper) {
        this.transactionalEnrichmentService = transactionalEnrichmentService;
        this.enrichmentPipelineService = enrichmentPipelineService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
    }

    @SqsListener("${app.sqs.enrichment-queue-name}")
    public void receiveMessage(MessagePayload payload) {
        try {
            logger.info("Received message from SQS for cleansedDataId: {}", payload.getCleansedDataId());

            CleansedDataStore cleansedDataStore = cleansedDataStoreRepository.findById(payload.getCleansedDataId())
                    .orElseThrow(() -> new IllegalStateException("CleansedDataStore not found with id: " + payload.getCleansedDataId()));

            Map<String, Object> cleansedItemMap = payload.getCleansedItem();
            EnrichmentContext context = objectMapper.convertValue(cleansedItemMap.get("context"), EnrichmentContext.class);

            CleansedItemDetail itemDetail = new CleansedItemDetail(
                    (String) cleansedItemMap.get("sourcePath"),
                    (String) cleansedItemMap.get("originalFieldName"),
                    (String) cleansedItemMap.get("cleansedContent"),
                    (String) cleansedItemMap.get("model"),
                    context
            );

            transactionalEnrichmentService.enrichItem(itemDetail, cleansedDataStore);

            enrichmentPipelineService.checkAndTriggerPostProcessing(payload.getCleansedDataId());
        } catch (Exception e) {
            logger.error("Failed to process SQS message for cleansedDataId: {}", payload.getCleansedDataId(), e);
        }
    }
}