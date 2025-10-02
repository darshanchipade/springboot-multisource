package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.service.EnrichmentResult;
import com.apple.springboot.model.JobTracker;
import com.apple.springboot.model.SQSMessage;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.JobTrackerRepository;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.apple.springboot.model.CleansedItemDetail;

import java.time.OffsetDateTime;

@Service
public class SqsEnrichmentWorker {

    private static final Logger logger = LoggerFactory.getLogger(SqsEnrichmentWorker.class);

    private final TransactionalEnrichmentService transactionalEnrichmentService;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final JobTrackerRepository jobTrackerRepository;
    private final JobCompletionService jobCompletionService;

    public SqsEnrichmentWorker(TransactionalEnrichmentService transactionalEnrichmentService,
                               CleansedDataStoreRepository cleansedDataStoreRepository,
                               JobTrackerRepository jobTrackerRepository,
                               JobCompletionService jobCompletionService) {
        this.transactionalEnrichmentService = transactionalEnrichmentService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.jobTrackerRepository = jobTrackerRepository;
        this.jobCompletionService = jobCompletionService;
    }

    @SqsListener(value = "${sqs.queue.name}")
    public void receiveMessage(SQSMessage message) {
        String jobId = message.getJobId();
        String sourcePath = message.getSourcePath();
        logger.info("Received SQS message for job ID: {} and source path: {}", jobId, sourcePath);

        boolean success = false;
        try {
            CleansedItemDetail itemDetail = new CleansedItemDetail(
                    message.getSourcePath(),
                    message.getOriginalFieldName(),
                    message.getCleansedContent(),
                    message.getModel(),
                    message.getContext()
            );

            CleansedDataStore cleansedDataStore = cleansedDataStoreRepository.findById(message.getCleansedDataStoreId())
                    .orElseThrow(() -> new RuntimeException("CleansedDataStore not found with ID: " + message.getCleansedDataStoreId()));

            //EnrichmentResult result = transactionalEnrichmentService.enrichItem(itemDetail, cleansedDataStore, jobId);
            EnrichmentResult result = transactionalEnrichmentService.enrichItem(itemDetail, cleansedDataStore);
            success = result.isSuccess();

            logger.info("Successfully processed SQS message for job ID: {} and source path: {}", jobId, sourcePath);

        } catch (Exception e) {
            logger.error("Failed to process SQS message for job ID: {} and source path: {}. Error: {}",
                    jobId, sourcePath, e.getMessage(), e);
            // Let the final block handle progress update.
        }

        // This call must be outside the main try-catch to ensure it always runs.
        // It's in its own transaction to guarantee atomicity.
        try {
            updateJobProgress(jobId, success);
        } catch (Exception e) {
            logger.error("CRITICAL: Failed to update job progress for job ID: {}. This may result in an incomplete job. Error: {}", jobId, e.getMessage(), e);
            // Re-throwing here will make the SQS message visible again for another attempt.
            throw new RuntimeException("Failed to update job progress for job ID: " + jobId, e);
        }
    }

    @Transactional
    public void updateJobProgress(String jobId, boolean success) {
        JobTracker jobTracker = jobTrackerRepository.findAndLockById(jobId)
                .orElseThrow(() -> new IllegalStateException("JobTracker not found for ID: " + jobId));

        jobTracker.setProcessedItems(jobTracker.getProcessedItems() + 1);
        if (success) {
            jobTracker.setSuccessCount(jobTracker.getSuccessCount() + 1);
        } else {
            jobTracker.setFailureCount(jobTracker.getFailureCount() + 1);
        }
        jobTracker.setUpdatedAt(OffsetDateTime.now());

        logger.debug("Job {} progress: {}/{}", jobId, jobTracker.getProcessedItems(), jobTracker.getTotalItems());

        if (jobTracker.getProcessedItems() >= jobTracker.getTotalItems()) {
            logger.info("All items for job {} have been processed. Finalizing job.", jobId);
            jobTracker.setStatus("FINALIZING");
            jobTrackerRepository.save(jobTracker); // Save progress before calling finalize
            jobCompletionService.finalizeJob(jobId);
        } else {
            jobTrackerRepository.save(jobTracker);
        }
    }
}