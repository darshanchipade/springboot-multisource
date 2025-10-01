package com.apple.springboot.controller;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.service.DataIngestionService;
import com.apple.springboot.service.EnrichmentPipelineService;
import com.apple.springboot.service.JobProgressService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class DataExtractionController {

    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);

    private final DataIngestionService dataIngestionService;
    private final EnrichmentPipelineService enrichmentPipelineService;
    private final JobProgressService jobProgressService;

    private final CleansedDataStoreRepository cleansedDataStoreRepository;

    // List of statuses from DataIngestionService that indicate a fatal error before enrichment stage,
    // or that processing should stop before enrichment.
    private static final List<String> PRE_ENRICHMENT_TERMINAL_STATUSES = Arrays.asList(
            "S3_FILE_NOT_FOUND_OR_EMPTY", "INVALID_S3_URI", "S3_DOWNLOAD_FAILED",
            "CLASSPATH_FILE_NOT_FOUND", "EMPTY_PAYLOAD", "SOURCE_EMPTY_PAYLOAD",
            "INVALID_S3_URI", "S3_DOWNLOAD_FAILED", "CLASSPATH_FILE_NOT_FOUND",
            "EMPTY_CONTENT_LOADED", // Added from DataIngestionService
            "CLASSPATH_READ_ERROR", // Added from DataIngestionService
            "JSON_PARSE_ERROR", "EXTRACTION_ERROR", "EXTRACTION_FAILED",
            "CLEANSING_SERIALIZATION_ERROR", "ERROR_SERIALIZING_ITEMS",
            "FILE_PROCESSING_ERROR", "FILE_ERROR"
    );

    @Autowired
    public DataExtractionController(DataIngestionService dataIngestionService,
                                    EnrichmentPipelineService enrichmentPipelineService,
                                    JobProgressService jobProgressService,
                                    CleansedDataStoreRepository cleansedDataStoreRepository) {
        this.dataIngestionService = dataIngestionService;
        this.enrichmentPipelineService = enrichmentPipelineService;
        this.jobProgressService = jobProgressService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
    }

    @GetMapping("/extract-cleanse-enrich-and-store")
    public ResponseEntity<?> extractCleanseEnrichAndStore(
            @RequestParam(name = "sourceUri", required = false) String sourceUriParam) {

        String identifierForServiceCall;
        String identifierForLog;

        if (sourceUriParam != null && !sourceUriParam.trim().isEmpty()) {
            identifierForServiceCall = sourceUriParam.trim();
            identifierForLog = identifierForServiceCall;
        } else {
            identifierForServiceCall = null; // DataIngestionService.ingestAndCleanseSingleFile() will use default
            identifierForLog = "default configured path";
        }

        logger.info("Received GET request to process from source: {}", identifierForLog);
        CleansedDataStore cleansedDataEntry = null;

        try {
            if (identifierForServiceCall != null) {
                cleansedDataEntry = dataIngestionService.ingestAndCleanseSingleFile(identifierForServiceCall);
            } else {
                cleansedDataEntry = dataIngestionService.ingestAndCleanseSingleFile();
            }
            // The ingestAndCleanseSingleFile method now returns CleansedDataStore directly or throws an exception.
            return handleIngestionAndTriggerEnrichment(cleansedDataEntry, identifierForLog);

        } catch (IOException | UncheckedIOException e) {
            logger.error("File I/O error for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "File I/O error for " + identifierForLog + ": " + e.getMessage()));
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Invalid input for " + identifierForLog + ": " + e.getMessage()));
        } catch (Exception e) {
            logger.error("Unexpected error for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Unexpected error for " + identifierForLog + ": " + e.getMessage()));
        }
    }

//        public void extractCleanseEnrichAndStore(
//            @RequestParam(name = "sourceUri", required = false) String sourceUriParam) {
//
//        String identifierForServiceCall;
//        String identifierForLog;
//
//        if (sourceUriParam != null && !sourceUriParam.trim().isEmpty()) {
//            identifierForServiceCall = sourceUriParam.trim();
//            identifierForLog = identifierForServiceCall;
//        } else {
//            identifierForServiceCall = null; // DataIngestionService.ingestAndCleanseSingleFile() will use default
//            identifierForLog = "default configured path";
//        }
//
//        logger.info("Received GET request to process from source: {}", identifierForLog);
//        CleansedDataStore cleansedDataEntry = null;
//
//        try {
//            if (identifierForServiceCall != null) {
//                cleansedDataEntry = dataIngestionService.ingestAndCleanseSingleFile(identifierForServiceCall);
//            } else {
//                cleansedDataEntry = dataIngestionService.ingestAndCleanseSingleFile();
//            }
//            // The ingestAndCleanseSingleFile method now returns CleansedDataStore directly or throws an exception.
//            return ;
//
//        } catch (UncheckedIOException e) {
//            logger.error("File I/O error for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
//            return ;
//        } catch (IllegalArgumentException e) {
//            logger.error("Invalid argument for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
//            return ;
//        } catch (Exception e) {
//            logger.error("Unexpected error for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
//            return ;
//        }
//    }

    @PostMapping("/ingest-json-payload")
    public ResponseEntity<?> ingestJsonPayload(@RequestBody String jsonPayload) {
        String sourceIdentifier = "api-payload-" + UUID.randomUUID().toString();
        logger.info("Received POST request to process JSON payload. Assigned sourceIdentifier: {}", sourceIdentifier);
        CleansedDataStore cleansedDataEntry = null;

        try {
            if (jsonPayload == null || jsonPayload.trim().isEmpty()) {
                logger.warn("Received empty JSON payload for identifier: {}", sourceIdentifier);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("JSON payload cannot be empty.");
            }

            cleansedDataEntry = dataIngestionService.ingestAndCleanseJsonPayload(jsonPayload, sourceIdentifier);
            return handleIngestionAndTriggerEnrichment(cleansedDataEntry, sourceIdentifier);

        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument processing JSON payload for identifier: {}. Error: {}", sourceIdentifier, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Invalid argument for payload " + sourceIdentifier + ": " + e.getMessage()));
        } catch (Exception e) {
            logger.error("Error processing JSON payload for identifier: {}. Error: {}", sourceIdentifier, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", "Error processing JSON payload "+ sourceIdentifier + ": " + e.getMessage()));
        }
    }
    @GetMapping("/cleansed-data-status/{id}")
    public String getStatus(@PathVariable UUID id) {
        return cleansedDataStoreRepository.findById(id)
                .map(CleansedDataStore::getStatus)
                .orElse("NOT_FOUND");
    }

    @GetMapping("/progress/{id}")
    public SseEmitter getProgress(@PathVariable String id) {
        SseEmitter emitter = jobProgressService.getEmitter(id);
        if (emitter == null) {
            // Optionally handle the case where the job ID is not found
            // For now, we can let the client handle a failed connection.
            logger.warn("No active progress emitter found for job ID: {}", id);
        }
        return emitter;
    }

    private ResponseEntity<?> handleIngestionAndTriggerEnrichment(CleansedDataStore cleansedDataEntry, String identifierForLog) {
        if (cleansedDataEntry == null || cleansedDataEntry.getId() == null) {
            String statusMsg = (cleansedDataEntry != null && cleansedDataEntry.getStatus() != null) ?
                    cleansedDataEntry.getStatus() : "Ingestion service returned null or ID-less CleansedDataStore.";
            logger.error("Data ingestion/cleansing failed for identifier: {}. Status: {}", identifierForLog, statusMsg);
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(Map.of("error", "Failed to process source: " + identifierForLog + ". Reason: " + statusMsg));
        }

        UUID cleansedDataStoreId = cleansedDataEntry.getId();
        String currentStatus = cleansedDataEntry.getStatus();
        logger.info("Ingestion/cleansing complete for identifier: {}. CleansedDataStore ID: {}, Status: {}",
                identifierForLog, cleansedDataStoreId, currentStatus);

        if (currentStatus != null && PRE_ENRICHMENT_TERMINAL_STATUSES.stream().anyMatch(s -> s.equalsIgnoreCase(currentStatus))) {
            logger.warn("Ingestion/cleansing for {} ended with status: {}. No enrichment will be triggered. Details: {}", identifierForLog, currentStatus, cleansedDataEntry.getCleansingErrors());
            HttpStatus httpStatus = (Arrays.asList("S3_FILE_NOT_FOUND_OR_EMPTY", "CLASSPATH_FILE_NOT_FOUND", "EMPTY_PAYLOAD", "SOURCE_EMPTY_PAYLOAD", "EMPTY_CONTENT_LOADED").contains(currentStatus.toUpperCase()))
                    ? HttpStatus.NOT_FOUND
                    : HttpStatus.UNPROCESSABLE_ENTITY;
            return ResponseEntity.status(httpStatus)
                    .body("Problem with input source or cleansing for: " + identifierForLog + ". Status: " + currentStatus + ". Details: " + cleansedDataEntry.getCleansingErrors());
        }

        if ("NO_CONTENT_EXTRACTED".equalsIgnoreCase(currentStatus) || "PROCESSED_EMPTY_ITEMS".equalsIgnoreCase(currentStatus)) {
            logger.info("Processing for {} completed with status: {}. No content for enrichment. CleansedDataID: {}", identifierForLog, currentStatus, cleansedDataStoreId);
            return ResponseEntity.ok(Map.of("message", "Source processed. No content extracted for enrichment.", "cleansedDataId", cleansedDataStoreId.toString(), "status", currentStatus));
        }

        if (!"CLEANSED_PENDING_ENRICHMENT".equalsIgnoreCase(currentStatus)) {
            logger.warn("Ingestion/cleansing for {} completed with status: '{}', which is not 'CLEANSED_PENDING_ENRICHMENT'. No enrichment will be triggered. CleansedDataID: {}. Errors: {}",
                    identifierForLog, currentStatus, cleansedDataStoreId, cleansedDataEntry.getCleansingErrors());
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED)
                    .body(Map.of("error", "Data ingestion/cleansing for " + identifierForLog + " resulted in status '" + currentStatus + "', cannot proceed to enrichment.", "details", cleansedDataEntry.getCleansingErrors()));
        }

        int totalItems = cleansedDataEntry.getCleansedItems() != null ? cleansedDataEntry.getCleansedItems().size() : 0;
        final var job = jobProgressService.createJob(totalItems);

        logger.info("Proceeding to enrichment for CleansedDataStore ID: {} from identifier: {}. Job ID: {}", cleansedDataStoreId, identifierForLog, job.getJobId());

        final CleansedDataStore finalCleansedDataEntry = cleansedDataEntry;
        new Thread(() -> {
            try {
                logger.info("Initiating asynchronous enrichment for CleansedDataStore ID: {}, Job ID: {}", finalCleansedDataEntry.getId(), job.getJobId());
                enrichmentPipelineService.enrichAndStore(finalCleansedDataEntry, job.getJobId());
            } catch (Exception e) {
                logger.error("Asynchronous enrichment failed for CleansedDataStore ID: {}. Error: {}", finalCleansedDataEntry.getId(), e.getMessage(), e);
                jobProgressService.updateProgress(job.getJobId(), "FATAL_ERROR: " + e.getMessage());
                jobProgressService.completeJob(job.getJobId());
            }
        }).start();

        Map<String, String> response = Map.of(
                "message", "Request for " + identifierForLog + " accepted. Enrichment processing initiated in background.",
                "jobId", job.getJobId(),
                "cleansedDataId", cleansedDataStoreId.toString(),
                "status", currentStatus,
                "progressUrl", "/api/progress/" + job.getJobId()
        );
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}