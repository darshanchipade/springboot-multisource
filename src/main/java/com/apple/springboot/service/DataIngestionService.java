package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.RawDataStore;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.RawDataStoreRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.*;


/**
 * Service responsible for ingesting JSON content from different sources (S3, classpath, API),
 * parsing and cleansing the content, and storing the raw and cleansed data into PostgreSQL tables.
 */
@Service
public class DataIngestionService {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionService.class);

    private final RawDataStoreRepository rawDataStoreRepository;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;
    private final ResourceLoader resourceLoader;
    private final String jsonFilePath; // Default classpath file path from properties
    private final S3StorageService s3StorageService;
    private final String defaultS3BucketName;

    public DataIngestionService(RawDataStoreRepository rawDataStoreRepository,
                                CleansedDataStoreRepository cleansedDataStoreRepository,
                                ObjectMapper objectMapper,
                                ResourceLoader resourceLoader,
                                @Value("${app.json.file.path}") String jsonFilePath,
                                S3StorageService s3StorageService,
                                @Value("${app.s3.bucket-name}") String defaultS3BucketName) {
        this.rawDataStoreRepository = rawDataStoreRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
        this.resourceLoader = resourceLoader;
        this.jsonFilePath = jsonFilePath;
        this.s3StorageService = s3StorageService;
        this.defaultS3BucketName = defaultS3BucketName;
    }

    // Helper class for S3 URI parsing
    private static class S3ObjectDetails {
        final String bucketName;
        final String fileKey;
        S3ObjectDetails(String bucketName, String fileKey) {
            this.bucketName = bucketName;
            this.fileKey = fileKey;
        }
    }
    
    /**
     * Parses an S3 URI string and extracts bucket name and key.
     *
     * @param s3Uri S3 URI is provided using post in format s3://bucket/key or s3:///key (for default bucket provided in configs).
     * @return Parsed S3ObjectDetails with bucket and key.
     * @throws IllegalArgumentException if format is invalid.
     */

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

    /**
     * Ingests and cleanses a raw JSON payload received directly from API
     *
     * @param jsonPayload     The raw JSON content as a string.
     * @param sourceIdentifier A unique identifier for the source of the data API or system name.
     * @return CleansedDataStore is used to the processed and structured data.
     * @throws JsonProcessingException if JSON parsing fails.
     */
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

    /**
     * Ingests and cleanses a JSON file from the application.properties 'app.json.file.path'
     * Uses both S3 and classpath-based sources.
     *
     * @return CleansedDataStore after processing and cleansing the file content.
     * @throws JsonProcessingException if JSON parsing fails.
     */
    @Transactional
    public CleansedDataStore ingestAndCleanseSingleFile() throws JsonProcessingException {
        try {
            return ingestAndCleanseSingleFile(this.jsonFilePath);
        } catch (IOException | RuntimeException e) {
            logger.error("Error processing default jsonFilePath '{}': {}. Creating error record.", this.jsonFilePath, e.getMessage(), e);
            String sourceUri = "";
            if (!sourceUri.startsWith("s3://") && !sourceUri.startsWith("classpath:")) {
                sourceUri = "classpath:" + sourceUri;
            } else {
                sourceUri = this.jsonFilePath;
            }
            String finalSourceUri = sourceUri;
            RawDataStore rawData = rawDataStoreRepository.findBySourceUri(sourceUri).orElseGet(() -> {
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

    // Main method for processing an identifier (S3 URI or classpath)
    /**
     * Ingests and cleanses content from a specific source identifier (S3 URI or classpath resource).
     *
     * @param identifier S3 URI (s3://bucket/key) or classpath resource path.
     * @return CleansedDataStore after cleansing extracted content.
     * @throws IOException if file reading or S3 download fails.
     */
    @Transactional
    public CleansedDataStore ingestAndCleanseSingleFile(String identifier) throws IOException {
        logger.info("Starting ingestion and cleansing for identifier: {}", identifier);
        String rawJsonContent;
        String sourceUriForDb = identifier;
        RawDataStore rawDataStore = new RawDataStore();
        rawDataStore.setSourceUri(sourceUriForDb);
        rawDataStore.setReceivedAt(OffsetDateTime.now());
        boolean isS3Source = false;

        if (identifier.startsWith("s3://")) {
            isS3Source = true;
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
            if(rawDataStore.getId() == null) rawDataStoreRepository.save(rawDataStore);
            return createAndSaveErrorCleansedDataStore(rawDataStore, "EMPTY_CONTENT_LOADED","Error" ,"ContentError: Loaded content was empty.");
        }

        rawDataStore.setRawContentText(rawJsonContent);
        rawDataStore.setRawContentBinary(rawJsonContent.getBytes(StandardCharsets.UTF_8));
        if(rawDataStore.getReceivedAt() == null) rawDataStore.setReceivedAt(OffsetDateTime.now());
        if(rawDataStore.getStatus() == null || rawDataStore.getStatus().equals("pending_cleansing")){
            rawDataStore.setStatus("RECEIVED");
        }
        RawDataStore savedRawDataStore = rawDataStoreRepository.save(rawDataStore);
        logger.info("Processed raw data with ID: {} for source: {} with status: {}", savedRawDataStore.getId(), sourceUriForDb, savedRawDataStore.getStatus());

        return processLoadedContent(rawJsonContent, sourceUriForDb, savedRawDataStore);
    }
    
    /**
     * Parses and extracts content like copy, disclaimer, analytics attributes from JSON.
     *
     * @param rawJsonContent      Raw JSON content string.
     * @param sourceUriForDb      The source URI recorded in the database.
     * @param associatedRawDataStore The corresponding RawDataStore entry.
     * @return CleansedDataStore with structured, extracted data.
     * @throws JsonProcessingException if parsing or serialization fails.
     */

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

        //cleansedDataStore.setCleansedItems(objectMapper.writeValueAsString(cleansedContentItems));
        cleansedDataStore.setCleansedItems(cleansedContentItems);

        if (cleansingErrorsJson != null) {
            cleansedDataStore.setCleansingErrors(objectMapper.valueToTree(cleansingErrorsJson));
        }

        if ("EXTRACTION_ERROR".equals(associatedRawDataStore.getStatus()) || "CLEANSING_SERIALIZATION_ERROR".equals(associatedRawDataStore.getStatus())) {
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

    // Helper to create and save error state for CleansedDataStore
    /**
     * Creates and saves an error record in the CleansedDataStore table when ingestion fails for any reason
     *
     * @param rawDataStore        Associated raw data record.
     * @param cleansedStatus      Status to be stored (e.gFILE_ERROR, S3_DOWNLOAD_FAILED).
     * @param errorKeyOrMessagePrefix Key or message prefix indicating error type.
     * @param specificErrorMessage     Detailed error message.
     * @return CleansedDataStore with error metadata.
     * @throws JsonProcessingException if serialization fails.
     */
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

    /**
     * Generates a basic error map with key and human-readable error message.
     *
     * @param errorKey     Identifier for the error.
     * @param errorMessage Detailed description of the error.
     * @return Map representation suitable for serialization.
     */
    private Map<String,Object> generateErrorJson(String errorKey, String errorMessage) {
        Map<String, Object> errorMap = new HashMap<>();
        errorMap.put("errorType", errorKey);
        errorMap.put("errorMessage", errorMessage != null ? errorMessage : "Unknown error");
        return errorMap;
    }
    
    /**
     * Recursively traverses a JSON and starts extracting 'copy', 'disclaimer', and 'analyticsAttributes'
     * into a list of content entries, each tagged with source path and model hint.
     *
     * @param node               Current JSON node.
     * @param currentPath        JSONPath-style representation of the node location.
     * @param inheritedModelHint Model hint from parent context.
     * @param inheritedPathValue Path value from parent node.
     * @param results            Collected list of cleansed items.
     */

    private static void findAndExtractRecursive(JsonNode node,
                                                String currentPath,
                                                String inheritedModelHint,
                                                String inheritedPathValue,
                                                List<Map<String, Object>> results) {
        if (node.isObject()) {
            String localModel = node.path("_model").asText(null);
            String localInheritedPath = node.path("_path").asText(null);
            String copyText = node.path("copy").asText(null);

            String model = (localModel != null && !localModel.isBlank()) ? localModel : inheritedModelHint;
            String semanticPath = (localInheritedPath != null && !localInheritedPath.isBlank()) ? localInheritedPath : inheritedPathValue;

            // Handle 'copy' field
            if (copyText != null && !copyText.isBlank()) {
                String cleansed = cleanseCopyText(copyText);
                if (!cleansed.isBlank()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("sourcePath", semanticPath != null ? semanticPath : currentPath + ".copy");
                    item.put("originalFieldName", "copy");
                    item.put("cleansedContent", cleansed);
                    if (model != null) item.put("model", model);
                    results.add(item);

                    logger.debug("Extracted copy: {}", item);
                }
            }
            // Handle 'disclaimer' field
            JsonNode disclaimerNode = node.get("disclaimer");
            if (disclaimerNode != null && disclaimerNode.isTextual()) {
                String disclaimerText = disclaimerNode.asText();
                String cleansed = cleanseCopyText(disclaimerText);

                if (!cleansed.isBlank()) {
                    Map<String, Object> item = new HashMap<>();
                    String itemSourcePath = (semanticPath != null && !semanticPath.isBlank())
                            ? semanticPath
                            : currentPath + ".disclaimer";

                    item.put("sourcePath", itemSourcePath);
                    item.put("originalFieldName", "disclaimer");
                    item.put("cleansedContent", cleansed);
                    if (model != null && !model.isBlank()) {
                        item.put("model", model);
                    }
                    results.add(item);

                    logger.debug("Extracted by 'disclaimer' rule: path='{}', modelHint='{}'", itemSourcePath, item.get("model"));
                }
            }
            // Handle analyticsAttributes
            if (node.has("analyticsAttributes") && node.get("analyticsAttributes").isArray()) {
                processAnalyticsAttributes(node.get("analyticsAttributes"), model, results);
            }

            // Recurse on other fields
            node.fields().forEachRemaining(entry -> {
                String fieldKey = entry.getKey();
                JsonNode value = entry.getValue();

                if (!Set.of("_model", "_path", "copy", "analyticsAttributes").contains(fieldKey)) {
                    String newPath = currentPath.equals("$") ? "$." + fieldKey : currentPath + "." + fieldKey;
                    findAndExtractRecursive(value, newPath, model, semanticPath, results);
                }
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                String newPath = currentPath + "[" + i + "]";
                findAndExtractRecursive(node.get(i), newPath, inheritedModelHint, inheritedPathValue, results);
            }
        }
    }
    
    /**
     * Processes 'analyticsAttributes' array field by extracting individual attributes
     * with associated name, value, path, and model. Since the analytics stored as name value pair
     *
     * @param analyticsArray   JSON array node containing analytics attributes.
     * @param currentModelHint Parent model hint inherited from higher-level context.
     * @param results          Target list to collect cleansed attribute items.
     */

    private static void processAnalyticsAttributes(JsonNode analyticsArray,
                                                   String currentModelHint,
                                                   List<Map<String, Object>> results) {
        for (JsonNode element : analyticsArray) {
            String path = element.path("_path").asText(null);
            String model = element.path("_model").asText(null);
            String name = element.path("name").asText(null);
            String value = element.path("value").asText(null);

            if (path != null && name != null && value != null && !value.isBlank()) {
                String cleansed = value.trim();
                if (!cleansed.isBlank()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("sourcePath", path);
                    item.put("originalFieldName", name);
                    item.put("cleansedContent", cleansed);
                    if (model != null && !model.isBlank()) {
                        item.put("model", model);
                    } else if (currentModelHint != null) {
                        item.put("model", currentModelHint);
                    }
                    results.add(item);

                    logger.debug("Extracted analytics attribute: {}", item);
                }
            }
        }
    }
    
    /**
     * Cleanses a given string of HTML tags, custom templating markers, and additional whitespaces.
     *
     * @param text Raw text to be cleaned.
     * @return A whitespace, tag-free string.
     */

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