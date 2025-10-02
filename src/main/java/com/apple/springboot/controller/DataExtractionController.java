package com.apple.springboot.controller;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.service.DataIngestionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class DataExtractionController {

    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);
    private final DataIngestionService dataIngestionService;

    public DataExtractionController(DataIngestionService dataIngestionService) {
        this.dataIngestionService = dataIngestionService;
    }

    @PostMapping("/ingest-and-cleanse")
    public ResponseEntity<?> ingestAndCleanseData(@RequestParam(required = false) String identifier) {
        try {
            CleansedDataStore cleansedData;
            if (identifier != null && !identifier.trim().isEmpty()) {
                logger.info("Starting ingestion from identifier: {}", identifier);
                cleansedData = dataIngestionService.ingestAndCleanseSingleFile(identifier);
            } else {
                logger.info("Starting ingestion from default file path.");
                cleansedData = dataIngestionService.ingestAndCleanseSingleFile();
            }
            return new ResponseEntity<>(Map.of("message", "Ingestion and cleansing complete. Enrichment has been queued.", "cleansedDataId", cleansedData.getId()), HttpStatus.ACCEPTED);
        } catch (IOException e) {
            logger.error("Error during ingestion: {}", e.getMessage(), e);
            return new ResponseEntity<>(Map.of("error", e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/ingest-payload")
    public ResponseEntity<?> ingestJsonPayload(@RequestBody String jsonPayload, @RequestParam String sourceIdentifier) {
        if (jsonPayload == null || jsonPayload.trim().isEmpty() || sourceIdentifier == null || sourceIdentifier.trim().isEmpty()) {
            return new ResponseEntity<>("JSON payload and sourceIdentifier are required.", HttpStatus.BAD_REQUEST);
        }
        try {
            CleansedDataStore cleansedData = dataIngestionService.ingestAndCleanseJsonPayload(jsonPayload, sourceIdentifier);
            return new ResponseEntity<>(Map.of("message", "Ingestion and cleansing complete. Enrichment has been queued.", "cleansedDataId", cleansedData.getId()), HttpStatus.ACCEPTED);
        } catch (Exception e) {
            logger.error("Error during payload ingestion: {}", e.getMessage(), e);
            return new ResponseEntity<>(Map.of("error", e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}