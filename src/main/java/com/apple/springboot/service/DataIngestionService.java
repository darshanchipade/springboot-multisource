package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.RawDataStore;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.RawDataStoreRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.*;

@Service
public class DataIngestionService {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionService.class);

    private final RawDataStoreRepository rawDataStoreRepository;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;
    private final ResourceLoader resourceLoader;
    private final String jsonFilePath;
    private final S3StorageService s3StorageService;
    private final String defaultS3BucketName;
    private final ContextConfigService contextConfigService;

    public DataIngestionService(RawDataStoreRepository rawDataStoreRepository,
                                CleansedDataStoreRepository cleansedDataStoreRepository,
                                ObjectMapper objectMapper,
                                ResourceLoader resourceLoader,
                                @Value("${app.json.file.path}") String jsonFilePath,
                                S3StorageService s3StorageService,
                                @Value("${app.s3.bucket-name}") String defaultS3BucketName,
                                ContextConfigService contextConfigService) {
        this.rawDataStoreRepository = rawDataStoreRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
        this.resourceLoader = resourceLoader;
        this.jsonFilePath = jsonFilePath;
        this.s3StorageService = s3StorageService;
        this.defaultS3BucketName = defaultS3BucketName;
        this.contextConfigService = contextConfigService;
    }

    private static class S3ObjectDetails {
        final String bucketName;
        final String fileKey;
        S3ObjectDetails(String bucketName, String fileKey) {
            this.bucketName = bucketName;
            this.fileKey = fileKey;
        }
    }

    private S3ObjectDetails parseS3Uri(String s3Uri) throws IllegalArgumentException {
        if (s3Uri == null || !s3Uri.startsWith("s3://")) {
            throw new IllegalArgumentException("Invalid S3 URI format: Must start with s3://. Received: " + s3Uri);
        }
        String pathPart = s3Uri.substring("s3://".length());
        if (pathPart.startsWith("///")) {
            String key = pathPart.substring(2);
            if (key.startsWith("/")) key = key.substring(1);
            if (key.isEmpty()) throw new IllegalArgumentException("Invalid S3 URI: Key is empty for default bucket URI " + s3Uri);
            return new S3ObjectDetails(defaultS3BucketName, key);
        } else {
            int firstSlashIndex = pathPart.indexOf('/');
            if (firstSlashIndex == -1 || firstSlashIndex == 0 || firstSlashIndex == pathPart.length() - 1) {
                throw new IllegalArgumentException("Invalid S3 URI format. Expected s3://bucket/key or s3:///key. Received: " + s3Uri);
            }
            String bucket = pathPart.substring(0, firstSlashIndex);
            String key = pathPart.substring(firstSlashIndex + 1);
            if (key.isEmpty()) throw new IllegalArgumentException("Invalid S3 URI: Key is empty for bucket '" + bucket + "' in URI " + s3Uri);
            return new S3ObjectDetails(bucket, key);
        }
    }

    @Transactional
    public CleansedDataStore ingestAndCleanseJsonPayload(String jsonPayload, String sourceIdentifier) throws JsonProcessingException {
        logger.info("Starting ingestion and cleansing for direct JSON payload with sourceIdentifier: {}", sourceIdentifier);
        RawDataStore rawDataStore = new RawDataStore();
        rawDataStore.setSourceUri(sourceIdentifier);
        rawDataStore.setReceivedAt(OffsetDateTime.now());

        if (jsonPayload == null || jsonPayload.trim().isEmpty()) {
            logger.warn("Received empty or null JSON payload for sourceIdentifier: {}.", sourceIdentifier);
            rawDataStore.setRawContentText(jsonPayload);
            rawDataStore.setRawContentBinary(new byte[0]);
            rawDataStore.setStatus("EMPTY_PAYLOAD");
            rawDataStoreRepository.save(rawDataStore);
            return createAndSaveErrorCleansedDataStore(rawDataStore, "SOURCE_EMPTY_PAYLOAD", "ERROR","PayloadError: Received empty or null JSON payload.");
        }

        rawDataStore.setRawContentText(jsonPayload);
        rawDataStore.setRawContentBinary(jsonPayload.getBytes(StandardCharsets.UTF_8));
        rawDataStore.setStatus("RECEIVED_API_PAYLOAD");
        RawDataStore savedRawDataStore = rawDataStoreRepository.save(rawDataStore);
        logger.info("Stored raw data from payload with ID: {} for sourceIdentifier: {}", savedRawDataStore.getId(), sourceIdentifier);

        return processLoadedContent(jsonPayload, sourceIdentifier, savedRawDataStore);
    }

    @Transactional
    public CleansedDataStore ingestAndCleanseSingleFile() throws JsonProcessingException {
        try {
            return ingestAndCleanseSingleFile(this.jsonFilePath);
        } catch (IOException | RuntimeException e) {
            logger.error("Error processing default jsonFilePath '{}': {}. Creating error record.", this.jsonFilePath, e.getMessage(), e);
            String sourceUri;
            if (!this.jsonFilePath.startsWith("s3://") && !this.jsonFilePath.startsWith("classpath:")) {
                sourceUri = "classpath:" + this.jsonFilePath;
            } else {
                sourceUri = this.jsonFilePath;
            }
            String finalSourceUri = sourceUri;
            RawDataStore rawData = rawDataStoreRepository.findBySourceUri(finalSourceUri).orElseGet(() -> {
                RawDataStore newRawData = new RawDataStore();
                newRawData.setSourceUri(finalSourceUri);
                return newRawData;
            });
            if(rawData.getReceivedAt() == null) rawData.setReceivedAt(OffsetDateTime.now());
            rawData.setStatus("FILE_PROCESSING_ERROR");
            rawData.setRawContentText("Error processing file: " + e.getMessage());
            rawDataStoreRepository.save(rawData);
            return createAndSaveErrorCleansedDataStore(rawData, "FILE_ERROR", "ERROR FROM FILE","FileProcessingError: " + e.getMessage());
        }
    }

    @Transactional
    public CleansedDataStore ingestAndCleanseSingleFile(String identifier) throws IOException {
        logger.info("Starting ingestion and cleansing for identifier: {}", identifier);
        String rawJsonContent;
        String sourceUriForDb = identifier;
        RawDataStore rawDataStore = new RawDataStore();
        rawDataStore.setSourceUri(sourceUriForDb);
        rawDataStore.setReceivedAt(OffsetDateTime.now());

        if (identifier.startsWith("s3://")) {
            logger.info("Identifier is an S3 URI: {}", sourceUriForDb);
            try {
                S3ObjectDetails s3Details = parseS3Uri(sourceUriForDb);
                rawJsonContent = s3StorageService.downloadFileContent(s3Details.bucketName, s3Details.fileKey);
                if (rawJsonContent == null) {
                    logger.warn("File not found or content is null from S3 URI: {}.", sourceUriForDb);
                    rawDataStore.setStatus("S3_FILE_NOT_FOUND_OR_EMPTY");
                    rawDataStoreRepository.save(rawDataStore);
                    return createAndSaveErrorCleansedDataStore(rawDataStore, "S3_FILE_NOT_FOUND_OR_EMPTY", "S3 ERROR", "S3Error: File not found or content was null at " + sourceUriForDb);
                }
                logger.info("Successfully downloaded content from S3 URI: {}", sourceUriForDb);
                rawDataStore.setStatus("S3_CONTENT_RECEIVED");
            } catch (IllegalArgumentException e) {
                logger.error("Invalid S3 URI format for identifier: '{}'. Error: {}", identifier, e.getMessage());
                rawDataStore.setStatus("INVALID_S3_URI");
                rawDataStore.setRawContentText("Invalid S3 URI: " + e.getMessage());
                rawDataStoreRepository.save(rawDataStore);
                return createAndSaveErrorCleansedDataStore(rawDataStore, "INVALID_S3_URI","INVALID S3", "InvalidS3URI: " + e.getMessage());
            } catch (Exception e) {
                logger.error("Failed to download S3 content for URI: '{}'. Error: {}", sourceUriForDb, e.getMessage(), e);
                rawDataStore.setStatus("S3_DOWNLOAD_FAILED");
                rawDataStore.setRawContentText("Error fetching S3 content: " + e.getMessage());
                rawDataStoreRepository.save(rawDataStore);
                return createAndSaveErrorCleansedDataStore(rawDataStore, "S3_DOWNLOAD_FAILED", "S3ERROR","S3DownloadError: " + e.getMessage());
            }
        } else {
            sourceUriForDb = identifier.startsWith("classpath:") ? identifier : "classpath:" + identifier;
            rawDataStore.setSourceUri(sourceUriForDb);
            logger.info("Identifier is a classpath resource: {}", sourceUriForDb);
            Resource resource = resourceLoader.getResource(sourceUriForDb);
            if (!resource.exists()) {
                logger.error("Classpath resource not found: {}", sourceUriForDb);
                rawDataStore.setStatus("CLASSPATH_FILE_NOT_FOUND");
                rawDataStoreRepository.save(rawDataStore);
                return createAndSaveErrorCleansedDataStore(rawDataStore, "CLASSPATH_FILE_NOT_FOUND", "FILE NOT FOUND","ClasspathError: File not found at " + sourceUriForDb);
            }
            try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
                rawJsonContent = FileCopyUtils.copyToString(reader);
            } catch (IOException e) {
                logger.error("Failed to read raw JSON file from classpath: {}", sourceUriForDb, e);
                rawDataStore.setStatus("CLASSPATH_READ_ERROR");
                rawDataStore.setRawContentText("Error reading classpath file: " + e.getMessage());
                rawDataStoreRepository.save(rawDataStore);
                return createAndSaveErrorCleansedDataStore(rawDataStore, "CLASSPATH_READ_ERROR", "READ ERROR","IOError: " + e.getMessage());
            }
            rawDataStore.setStatus("CLASSPATH_CONTENT_RECEIVED");
        }

        if (rawJsonContent.trim().isEmpty()) {
            logger.warn("Raw JSON content from {} is effectively empty after loading.", sourceUriForDb);
            rawDataStore.setRawContentText(rawJsonContent);
            rawDataStore.setStatus("EMPTY_CONTENT_LOADED");
            RawDataStore savedForEmpty = rawDataStoreRepository.save(rawDataStore);
            return createAndSaveErrorCleansedDataStore(savedForEmpty, "EMPTY_CONTENT_LOADED","Error" ,"ContentError: Loaded content was empty.");
        }

        rawDataStore.setRawContentText(rawJsonContent);
        rawDataStore.setRawContentBinary(rawJsonContent.getBytes(StandardCharsets.UTF_8));
        RawDataStore savedRawDataStore = rawDataStoreRepository.save(rawDataStore);
        logger.info("Processed raw data with ID: {} for source: {} with status: {}", savedRawDataStore.getId(), sourceUriForDb, savedRawDataStore.getStatus());

        return processLoadedContent(rawJsonContent, sourceUriForDb, savedRawDataStore);
    }

    private CleansedDataStore processLoadedContent(String rawJsonContent, String sourceUriForDb, RawDataStore associatedRawDataStore) throws JsonProcessingException {
        List<Map<String, Object>> cleansedContentItems = new ArrayList<>();
        Map<String,Object> cleansingErrorsJson = null;

        try {
            JsonNode rootNode = objectMapper.readTree(rawJsonContent);
            findAndExtractRecursive(rootNode, "$", null, null, cleansedContentItems);
            logger.info("Recursive parsing complete. Found {} processable items from raw data ID: {}", cleansedContentItems.size(), associatedRawDataStore.getId());
            associatedRawDataStore.setStatus("PROCESSED_FOR_CLEANSING");
        } catch (Exception e) {
            logger.error("Error during parsing/extraction for raw data ID: {}. Error: {}", associatedRawDataStore.getId(), e.getMessage(), e);
            associatedRawDataStore.setStatus("EXTRACTION_ERROR");
            cleansingErrorsJson = generateErrorJson("extractionOrParsingError", e instanceof JsonProcessingException ? "JSON parsing issue: " + e.getMessage() : "Text extraction issue: " + e.getMessage());
            rawDataStoreRepository.save(associatedRawDataStore);
            return createAndSaveErrorCleansedDataStore(associatedRawDataStore, "EXTRACTION_FAILED", "EXTRACTION ERROR","ExtractionError: " + e.getMessage());
        }

        CleansedDataStore cleansedDataStore = new CleansedDataStore();
        cleansedDataStore.setRawDataId(associatedRawDataStore.getId());
        cleansedDataStore.setSourceUri(sourceUriForDb);
        cleansedDataStore.setCleansedAt(OffsetDateTime.now());
        cleansedDataStore.setCleansedItems(cleansedContentItems);

        if (cleansingErrorsJson != null) {
            cleansedDataStore.setCleansingErrors(cleansingErrorsJson);
        }

        if ("EXTRACTION_ERROR".equals(associatedRawDataStore.getStatus())) {
            cleansedDataStore.setStatus("CLEANSING_FAILED");
        } else if (cleansedContentItems.isEmpty()) {
            logger.info("No content items extracted for raw_data_id: {}. Status set to 'NO_CONTENT_EXTRACTED'.", associatedRawDataStore.getId());
            cleansedDataStore.setStatus("NO_CONTENT_EXTRACTED");
            associatedRawDataStore.setStatus("PROCESSED_EMPTY_ITEMS");
        } else {
            cleansedDataStore.setStatus("CLEANSED_PENDING_ENRICHMENT");
            associatedRawDataStore.setStatus("CLEANSING_COMPLETE");
        }

        rawDataStoreRepository.save(associatedRawDataStore);
        return cleansedDataStoreRepository.save(cleansedDataStore);
    }

    private CleansedDataStore createAndSaveErrorCleansedDataStore(RawDataStore rawDataStore, String cleansedStatus, String errorKeyOrMessagePrefix, String specificErrorMessage) throws JsonProcessingException {
        CleansedDataStore errorCleansedData = new CleansedDataStore();
        if (rawDataStore != null && rawDataStore.getId() != null) {
            errorCleansedData.setRawDataId(rawDataStore.getId());
        }
        errorCleansedData.setSourceUri(rawDataStore != null ? rawDataStore.getSourceUri() : "unknown_source");
        errorCleansedData.setCleansedItems(Collections.emptyList());
        errorCleansedData.setStatus(cleansedStatus);
        String errorMessage = errorKeyOrMessagePrefix + (specificErrorMessage != null && !specificErrorMessage.isEmpty() ? ": " + specificErrorMessage : "");
        errorCleansedData.setCleansingErrors(generateErrorJson("IngestionCleansingError", errorMessage));
        errorCleansedData.setCleansedAt(OffsetDateTime.now());
        return cleansedDataStoreRepository.save(errorCleansedData);
    }

    private Map<String,Object> generateErrorJson(String errorKey, String errorMessage) {
        Map<String, Object> errorMap = new HashMap<>();
        errorMap.put("errorType", errorKey);
        errorMap.put("errorMessage", errorMessage != null ? errorMessage : "Unknown error");
        return errorMap;
    }

    private void findAndExtractRecursive(JsonNode currentNode,
                                         String currentJsonPath,
                                         String inheritedModel,
                                         String inheritedSourcePath,
                                         List<Map<String, Object>> results) {
        if (currentNode.isObject()) {
            String currentModel = currentNode.path("_model").asText(inheritedModel);
            String currentSourcePath = currentNode.path("_path").asText(inheritedSourcePath);

            // *** MODIFIED: Use 'context' as key, variable name is 'determinedContext' for clarity ***
            Map<String, Object> determinedContext = this.contextConfigService.getContext(currentModel, currentSourcePath != null ? currentSourcePath : currentJsonPath);
            // Direct "copy" field — textual content
            JsonNode copyNode = currentNode.get("copy");
            if (copyNode != null && copyNode.isTextual()) {
                String copyText = copyNode.asText();
                if (!copyText.isBlank()) {
                    String cleansed = cleanseCopyText(copyText);
                    if (!cleansed.isBlank()) {
                        Map<String, Object> item = new HashMap<>();
                        item.put("sourcePath", currentSourcePath != null ? currentSourcePath : currentJsonPath + ".copy");
                        item.put("originalFieldName", "copy");
                        item.put("cleansedContent", cleansed);
                        if (currentModel != null) item.put("model", currentModel);
                        // *** MODIFIED: Use 'context' as key ***
                        item.put("context", new HashMap<>(determinedContext));
                        results.add(item);
                        logger.debug("Extracted copy: {} with context: {}", item, determinedContext);
                    }
                }
            }
            // Direct "disclaimer" field — textual content
            JsonNode disclaimerNode = currentNode.get("disclaimer");
            if (disclaimerNode != null && disclaimerNode.isTextual()) {
                String disclaimerText = disclaimerNode.asText();
                String cleansed = cleanseCopyText(disclaimerText);
                if (!cleansed.isBlank()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("sourcePath", currentSourcePath != null ? currentSourcePath : currentJsonPath + ".disclaimer");
                    item.put("originalFieldName", "disclaimer");
                    item.put("cleansedContent", cleansed);
                    if (currentModel != null) item.put("model", currentModel);
                    // *** MODIFIED: Use 'context' as key ***
                    item.put("context", new HashMap<>(determinedContext));
                    results.add(item);
                    logger.debug("Extracted disclaimer: {} with context: {}", item, determinedContext);
                }
            }
            //"analyticsAttributes" array
            JsonNode analyticsArrayNode = currentNode.get("analyticsAttributes");
            if (analyticsArrayNode != null && analyticsArrayNode.isArray()) {
                // Pass the parent's determined context as a potential fallback/reference if needed
                processAnalyticsAttributes(analyticsArrayNode, currentModel, currentSourcePath != null ? currentSourcePath : currentJsonPath, results);
            }

            // Recurse into all other fields — including "copy" when it's an object/array, and "_meta" etc.
            currentNode.fields().forEachRemaining(entry -> {
                String fieldKey = entry.getKey();
                JsonNode fieldValue = entry.getValue();
                //commented for the nested copy
                // if (!Set.of("_model", "_path", "copy", "analyticsAttributes", "disclaimer").contains(fieldKey)) {
                // Only skip _model and _path — recurse into all others, including "copy", "disclaimer", etc.
                if (!Set.of("_model", "_path").contains(fieldKey)) {
                    String newJsonPath = currentJsonPath.equals("$") ? "$." + fieldKey : currentJsonPath + "." + fieldKey;
                    findAndExtractRecursive(fieldValue, newJsonPath, currentModel, currentSourcePath, results);
                }
            });
        } else if (currentNode.isArray()) {
            for (int i = 0; i < currentNode.size(); i++) {
                String newJsonPath = currentJsonPath + "[" + i + "]";
                findAndExtractRecursive(currentNode.get(i), newJsonPath, inheritedModel, inheritedSourcePath, results);
            }
        }
    }

    private void processAnalyticsAttributes(JsonNode analyticsArray,
                                            String parentModelHint,
                                            String parentPathHint,
                                            List<Map<String, Object>> results) {
        for (JsonNode element : analyticsArray) {
            if (element.isObject()) {
                String analyticsPath = element.path("_path").asText(parentPathHint);
                String analyticsModel = element.path("_model").asText(parentModelHint);
                String name = element.path("name").asText(null);
                String value = element.path("value").asText(null);

                // *** MODIFIED: Use 'context' as key, variable name is 'itemContext' for clarity ***
                Map<String, Object> itemContext = this.contextConfigService.getContext(analyticsModel, analyticsPath);

                if (analyticsPath != null && name != null && value != null && !value.isBlank()) {
                    String cleansedValue = value.trim();
                    if (!cleansedValue.isBlank()) {
                        Map<String, Object> item = new HashMap<>();
                        item.put("sourcePath", analyticsPath);
                        item.put("originalFieldName", name);
                        item.put("cleansedContent", cleansedValue);

                        if (analyticsModel != null) {
                            item.put("model", analyticsModel);
                        }
                        // *** MODIFIED: Use 'context' as key ***
                        item.put("context", new HashMap<>(itemContext));
                        results.add(item);
                       logger.debug("Extracted analytics attribute: {} with context: {}", item, itemContext);
                    }
                }
            }
        }
    }

    private static String cleanseCopyText(String text) {
        if (text == null) {
            return null;
        }
        String cleansed = text.replaceAll("\\{%.*?%\\}", " ");
        cleansed = cleansed.replaceAll("<[^>]+?>", " ");
        cleansed = cleansed.replaceAll("\\s+", " ").trim();
        return cleansed;
    }
}