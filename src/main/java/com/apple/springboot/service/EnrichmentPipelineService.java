package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.EnrichmentContext;
import com.apple.springboot.model.JobTracker;
import com.apple.springboot.model.SQSMessage;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.JobTrackerRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class EnrichmentPipelineService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentPipelineService.class);

    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;
    private final SqsEnrichmentPublisher sqsEnrichmentPublisher;
    private final JobTrackerRepository jobTrackerRepository;
    private final JobProgressService jobProgressService;

    public EnrichmentPipelineService(CleansedDataStoreRepository cleansedDataStoreRepository,
                                     ObjectMapper objectMapper,
                                     SqsEnrichmentPublisher sqsEnrichmentPublisher,
                                     JobTrackerRepository jobTrackerRepository,
                                     JobProgressService jobProgressService) {
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
        this.sqsEnrichmentPublisher = sqsEnrichmentPublisher;
        this.jobTrackerRepository = jobTrackerRepository;
        this.jobProgressService = jobProgressService;
    }

    public void enrichAndStore(CleansedDataStore cleansedDataEntry, String jobId) {
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
        if (itemsToEnrich.isEmpty()) {
            logger.info("No valid items to process for CleansedDataStore ID: {}. Marking as ENRICHED_NO_ITEMS_TO_PROCESS.", cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHED_NO_ITEMS_TO_PROCESS");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            return;
        }

        // Create and save the JobTracker
        JobTracker jobTracker = new JobTracker();
        jobTracker.setJobId(jobId);
        jobTracker.setCleansedDataStoreId(cleansedDataStoreId);
        jobTracker.setTotalItems(itemsToEnrich.size());
        jobTracker.setStatus("PENDING");
        jobTracker.setCreatedAt(OffsetDateTime.now());
        jobTracker.setUpdatedAt(OffsetDateTime.now());
        jobTrackerRepository.save(jobTracker);

        jobProgressService.updateProgress(jobId, "Created JobTracker. Found " + itemsToEnrich.size() + " items to process. Publishing to SQS.");

        // Publish each item to the SQS queue
        for (CleansedItemDetail item : itemsToEnrich) {
            SQSMessage sqsMessage = new SQSMessage(
                    jobId,
                    cleansedDataStoreId,
                    item.sourcePath,
                    item.originalFieldName,
                    item.cleansedContent,
                    item.model,
                    item.context,
                    itemsToEnrich.size()
            );
            sqsEnrichmentPublisher.publishEnrichmentRequest(sqsMessage);
        }

        logger.info("Successfully published all {} enrichment requests to SQS for job ID: {}", itemsToEnrich.size(), jobId);
        jobProgressService.updateProgress(jobId, "All " + itemsToEnrich.size() + " items have been queued for enrichment.");
        // The job is now fully asynchronous. The SqsEnrichmentWorker will handle the rest.
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