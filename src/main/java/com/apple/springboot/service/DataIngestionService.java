package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentHashRepository;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class DataIngestionService {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionService.class);
    private static final Pattern LOCALE_PATTERN = Pattern.compile("/([a-z]{2}_[A-Z]{2})/");

    private final RawDataStoreRepository rawDataStoreRepository;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;
    private final ResourceLoader resourceLoader;
    private final String jsonFilePath;
    private final S3StorageService s3StorageService;
    private final String defaultS3BucketName;
    private final ContentHashRepository contentHashRepository;
    private final ContextUpdateService contextUpdateService;
    /**
     * Constructs the service with required repositories and config values.
     */
    public DataIngestionService(RawDataStoreRepository rawDataStoreRepository,
                                CleansedDataStoreRepository cleansedDataStoreRepository,
                                ContentHashRepository contentHashRepository,
                                ObjectMapper objectMapper,
                                ResourceLoader resourceLoader,
                                @Value("${app.json.file.path}") String jsonFilePath,
                                S3StorageService s3StorageService,
                                @Value("${app.s3.bucket-name}") String defaultS3BucketName,
                                ContextUpdateService contextUpdateService) {
        this.rawDataStoreRepository = rawDataStoreRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.contentHashRepository = contentHashRepository;
        this.contextUpdateService = contextUpdateService;
        this.objectMapper = objectMapper;
        this.resourceLoader = resourceLoader;
        this.jsonFilePath = jsonFilePath;
        this.s3StorageService = s3StorageService;
        this.defaultS3BucketName = defaultS3BucketName;
    }


    private static class S3ObjectDetails {
        final String bucketName;
        final String fileKey;
        S3ObjectDetails(String bucketName, String fileKey) {
            this.bucketName = bucketName;
            this.fileKey = fileKey;
        }
    }

    /**
     * Helper method to parse S3 URI and return bucket + key.
     * Supports s3://bucket/key and s3:///key (uses default bucket).
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
     * Ingests a JSON string from REST api, stores it as raw data if not duplicate,
     * and processes it to produce cleansed content.
     */
    @Transactional
    public CleansedDataStore ingestAndCleanseJsonPayload(String jsonPayload, String sourceIdentifier) throws JsonProcessingException {
        logger.info("Starting ingestion for sourceIdentifier: {}", sourceIdentifier);

        if (jsonPayload == null || jsonPayload.trim().isEmpty()) {
            logger.warn("Received empty or null JSON payload for sourceIdentifier: {}.", sourceIdentifier);
            RawDataStore rawData = new RawDataStore();
            rawData.setSourceUri(sourceIdentifier);
            rawData.setReceivedAt(OffsetDateTime.now());
            rawData.setRawContentText(jsonPayload);
            rawData.setStatus("EMPTY_PAYLOAD");
            rawDataStoreRepository.save(rawData);
            return createAndSaveErrorCleansedDataStore(rawData, "SOURCE_EMPTY_PAYLOAD", "ERROR", "PayloadError: Received empty or null JSON payload.");
        }

        String newContentHash = calculateContentHash(jsonPayload, null);
        List<RawDataStore> latestVersionList = rawDataStoreRepository.findTopBySourceUriOrderByVersionDesc(sourceIdentifier);

        RawDataStore rawDataStore;
        if (!latestVersionList.isEmpty()) {
            rawDataStore = latestVersionList.get(0);
            if (Objects.equals(rawDataStore.getContentHash(), newContentHash)) {
                logger.info("Ingested content for sourceIdentifier '{}' has not changed. Skipping processing.", sourceIdentifier);
                return cleansedDataStoreRepository.findByRawDataId(rawDataStore.getId()).orElse(null);
            }
            logger.info("Found existing RawDataStore with ID {}. Content has changed, updating record.", rawDataStore.getId());
        } else {
            rawDataStore = new RawDataStore();
            rawDataStore.setSourceUri(sourceIdentifier);
            rawDataStore.setVersion(1); // First version
            logger.info("No existing RawDataStore found for sourceIdentifier {}. Creating a new one.", sourceIdentifier);
        }

        rawDataStore.setReceivedAt(OffsetDateTime.now());
        rawDataStore.setRawContentText(jsonPayload);
        rawDataStore.setRawContentBinary(jsonPayload.getBytes(StandardCharsets.UTF_8));
        rawDataStore.setContentHash(newContentHash);
        rawDataStore.setStatus("RECEIVED_API_PAYLOAD");
        rawDataStore.setSourceContentType("application/json");

        try {
            JsonNode rootNode = objectMapper.readTree(jsonPayload);
            ((com.fasterxml.jackson.databind.node.ObjectNode) rootNode).remove("_model");
            ((com.fasterxml.jackson.databind.node.ObjectNode) rootNode).remove("_path");
            ((com.fasterxml.jackson.databind.node.ObjectNode) rootNode).remove("copy");
            rawDataStore.setSourceMetadata(objectMapper.writeValueAsString(rootNode));
        } catch (JsonProcessingException e) {
            logger.error("Error processing JSON payload to extract metadata", e);
        }

        RawDataStore savedRawDataStore = rawDataStoreRepository.save(rawDataStore);
        logger.info("Saved RawDataStore ID: {} for sourceIdentifier: {}", savedRawDataStore.getId(), sourceIdentifier);

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
        logger.info("Starting ingestion for identifier: {}", identifier);
        String rawJsonContent;
        String sourceUriForDb = identifier;

        // Simplified content loading logic
        try {
            if (identifier.startsWith("s3://")) {
                S3ObjectDetails s3Details = parseS3Uri(sourceUriForDb);
                rawJsonContent = s3StorageService.downloadFileContent(s3Details.bucketName, s3Details.fileKey);
            } else {
                sourceUriForDb = identifier.startsWith("classpath:") ? identifier : "classpath:" + identifier;
                Resource resource = resourceLoader.getResource(sourceUriForDb);
                if (!resource.exists()) throw new IOException("Classpath resource not found: " + sourceUriForDb);
                try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
                    rawJsonContent = FileCopyUtils.copyToString(reader);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to load content for identifier '{}': {}", identifier, e.getMessage(), e);
            // Create a basic error record
            RawDataStore errorStore = new RawDataStore();
            errorStore.setSourceUri(sourceUriForDb);
            errorStore.setReceivedAt(OffsetDateTime.now());
            errorStore.setStatus("CONTENT_LOAD_ERROR");
            errorStore.setRawContentText("Failed to load content: " + e.getMessage());
            rawDataStoreRepository.save(errorStore);
            return createAndSaveErrorCleansedDataStore(errorStore, "CONTENT_LOAD_ERROR", "ERROR", "ContentLoadError: " + e.getMessage());
        }

        if (rawJsonContent == null || rawJsonContent.trim().isEmpty()) {
            logger.warn("Raw JSON content from {} is effectively empty after loading.", sourceUriForDb);
            // Similar empty payload handling as ingestAndCleanseJsonPayload
            return ingestAndCleanseJsonPayload("", sourceUriForDb);
        }

        // The rest of the logic is now identical to ingestAndCleanseJsonPayload
        return ingestAndCleanseJsonPayload(rawJsonContent, sourceUriForDb);
    }

    private CleansedDataStore processLoadedContent(String rawJsonContent, String sourceUriForDb, RawDataStore associatedRawDataStore) throws JsonProcessingException {
        List<Map<String, Object>> cleansedContentItems = new ArrayList<>();
        Map<String,Object> cleansingErrorsJson = null;

        try {
            JsonNode rootNode = objectMapper.readTree(rawJsonContent);
            Envelope rootEnvelope = new Envelope();
            rootEnvelope.setSourcePath(sourceUriForDb); // Set the root source path
            findAndExtractRecursive(rootNode, "$", rootEnvelope, new Facets(), cleansedContentItems);
            logger.debug("Recursive parsing complete. Found {} processable items from raw data ID: {}", cleansedContentItems.size(), associatedRawDataStore.getId());
            associatedRawDataStore.setStatus("PROCESSED_FOR_CLEANSING");
        } catch (Exception e) {
            logger.error("Error during parsing/extraction for raw data ID: {}. Error: {}", associatedRawDataStore.getId(), e.getMessage(), e);
            associatedRawDataStore.setStatus("EXTRACTION_ERROR");
            cleansingErrorsJson = generateErrorJson("extractionOrParsingError", e instanceof JsonProcessingException ? "JSON parsing issue: " + e.getMessage() : "Text extraction issue: " + e.getMessage());
            rawDataStoreRepository.save(associatedRawDataStore);
            return createAndSaveErrorCleansedDataStore(associatedRawDataStore, "EXTRACTION_FAILED", "EXTRACTION ERROR","ExtractionError: " + e.getMessage());
        }
        // Step 1: Collect all primary keys from the extracted items
        List<ContentHashId> ids = cleansedContentItems.stream()
                .map(item -> new ContentHashId((String) item.get("sourcePath"), (String) item.get("itemType")))
                .toList();

        // Step 2: Perform a single bulk query to fetch all existing entities
        Map<ContentHashId, ContentHash> existingHashesMap = contentHashRepository.findAllById(ids).stream()
                .collect(Collectors.toMap(
                        contentHash -> new ContentHashId(contentHash.getSourcePath(), contentHash.getItemType()),
                        contentHash -> contentHash
                ));

        List<Map<String, Object>> itemsForCleansedStore = new ArrayList<>();

        // Step 3: Iterate through items, compare with the fetched map, and update/create
        for (Map<String, Object> item : cleansedContentItems) {
            String sourcePath = (String) item.get("sourcePath");
            String itemType = (String) item.get("itemType");
            String contentHash = (String) item.get("contentHash");
            String contextHash = (String) item.get("contextHash");
            ContentHashId currentId = new ContentHashId(sourcePath, itemType);

            ContentHash existingHash = existingHashesMap.get(currentId);

            if (existingHash != null) {
                // Entity exists, check if update is needed
                boolean contentChanged = !Objects.equals(existingHash.getContentHash(), contentHash);
                boolean contextChanged = contextHash != null && !Objects.equals(existingHash.getContextHash(), contextHash);

                if (contentChanged || contextChanged) {
                    itemsForCleansedStore.add(item);
                    if (contentChanged) {
                        existingHash.setContentHash(contentHash);
                    }
                    if (contextChanged) {
                        existingHash.setContextHash(contextHash);
                    }
                }
            } else {
                // Entity does not exist, it's new
                itemsForCleansedStore.add(item);
                contentHashRepository.save(new ContentHash(sourcePath, itemType, contentHash, contextHash));
            }
        }

        if (itemsForCleansedStore.isEmpty()) {
            logger.info("No new or updated content to process for raw_data_id: {}", associatedRawDataStore.getId());
            associatedRawDataStore.setStatus("PROCESSED_NO_CHANGES");
            rawDataStoreRepository.save(associatedRawDataStore);
            return cleansedDataStoreRepository.findByRawDataId(associatedRawDataStore.getId()).orElse(null);
        }

        CleansedDataStore cleansedDataStore = new CleansedDataStore();
        cleansedDataStore.setRawDataId(associatedRawDataStore.getId());
        cleansedDataStore.setSourceUri(sourceUriForDb);
        cleansedDataStore.setCleansedAt(OffsetDateTime.now());
        cleansedDataStore.setCleansedItems(itemsForCleansedStore);
        cleansedDataStore.setVersion(associatedRawDataStore.getVersion());

        cleansedDataStore.setContext(new HashMap<>());

        if (cleansingErrorsJson != null) {
            cleansedDataStore.setCleansingErrors(cleansingErrorsJson);
        }

        if ("EXTRACTION_ERROR".equals(associatedRawDataStore.getStatus())) {
            cleansedDataStore.setStatus("CLEANSING_FAILED");
        } else if (itemsForCleansedStore.isEmpty())  {
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

    /**
     * Computes a SHA-256 hash of the JSON + optional context for deduplication.
     */
    private String calculateContentHash(String content, String context) {
        if (content == null || content.isEmpty()) {
            return null;
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(content.getBytes(StandardCharsets.UTF_8));
            if (context != null && !context.isEmpty()) {
                digest.update(context.getBytes(StandardCharsets.UTF_8));
            }
            byte[] encodedhash = digest.digest();
            return bytesToHex(encodedhash);
        } catch (NoSuchAlgorithmException e) {
            logger.error("SHA-256 algorithm not found", e);
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    /**
     * Creates a fallback CleansedDataStore record in case of ingestion/cleansing failure.
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

    private Map<String,Object> generateErrorJson(String errorKey, String errorMessage) {
        Map<String, Object> errorMap = new HashMap<>();
        errorMap.put("errorType", errorKey);
        errorMap.put("errorMessage", errorMessage != null ? errorMessage : "Unknown error");
        return errorMap;
    }

    private void findAndExtractRecursive(JsonNode currentNode,
                                         String currentJsonPath,
                                         Envelope parentEnvelope,
                                         Facets parentFacets,
                                         List<Map<String, Object>> results) {

        if (currentNode.isObject()) {
            Envelope currentEnvelope = (parentEnvelope != null)
                    ? objectMapper.convertValue(objectMapper.convertValue(parentEnvelope, Map.class), Envelope.class)
                    : new Envelope();

            Facets currentFacets = new Facets();
            if (parentFacets != null) {
                currentFacets.putAll(parentFacets);
            }

            // Populate current context from the node's fields
            currentNode.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                if (value.isValueNode() && !key.startsWith("_")) {
                    currentFacets.put(key, value.asText());
                }
            });

            String currentModel = currentNode.path("_model").asText(parentEnvelope != null ? parentEnvelope.getModel() : null);
            currentEnvelope.setModel(currentModel);

            String currentSourcePath = currentNode.path("_path").asText(parentEnvelope != null ? parentEnvelope.getSourcePath() : null);
            currentEnvelope.setSourcePath(currentSourcePath);

            if (currentSourcePath != null) {
                Matcher matcher = LOCALE_PATTERN.matcher(currentSourcePath);
                if (matcher.find()) {
                    String locale = matcher.group(1);
                    currentEnvelope.setLocale(locale);
                    String[] parts = locale.split("_");
                    if (parts.length == 2) {
                        currentEnvelope.setLanguage(parts[0]);
                        currentEnvelope.setCountry(parts[1]);
                    }
                }
            }

            List<String> pathHierarchy = new ArrayList<>(parentEnvelope.getPathHierarchy() != null ? parentEnvelope.getPathHierarchy() : Collections.emptyList());
            if (currentSourcePath != null && !currentSourcePath.equals(parentEnvelope.getSourcePath())) {
                String[] segments = currentSourcePath.split("/");
                if (segments.length > 0) {
                    String lastSegment = segments[segments.length - 1];
                    if (!lastSegment.isEmpty()) {
                        pathHierarchy.add(lastSegment);
                    }
                }
            }
            currentEnvelope.setPathHierarchy(pathHierarchy);

            final Set<String> contentFieldKeys = Set.of("copy", "disclaimer", "analytics");

            currentNode.fields().forEachRemaining(entry -> {
                String fieldKey = entry.getKey();
                JsonNode fieldValue = entry.getValue();

                if (contentFieldKeys.contains(fieldKey)) {
                    String cleansedContent = null;
                    Facets itemFacets = new Facets(); // Clone for this specific item
                    itemFacets.putAll(currentFacets);

                    if (fieldKey.equals("analytics") && fieldValue.isObject()) {
                        String name = fieldValue.path("name").asText(null);
                        String value = fieldValue.path("value").asText(null);
                        if (name != null && value != null) {
                            cleansedContent = cleanseCopyText(value);
                            itemFacets.put("analyticsName", name);
                        }
                    } else if (fieldValue.isTextual()) {
                        cleansedContent = cleanseCopyText(fieldValue.asText());
                    }

                    if (cleansedContent != null && !cleansedContent.isBlank()) {
                        EnrichmentContext finalContext = new EnrichmentContext(currentEnvelope, itemFacets);
                        Map<String, Object> item = new HashMap<>();
                        // Prefer the most specific path, but fall back to the parent's path.
                        String itemSourcePath = currentSourcePath != null ? currentSourcePath : parentEnvelope.getSourcePath();
                        item.put("sourcePath", itemSourcePath);
                        item.put("itemType", fieldKey); // e.g., "copy", "disclaimer"
                        item.put("originalFieldName", fieldKey);
                        item.put("cleansedContent", cleansedContent);
                        item.put("contentHash", calculateContentHash(cleansedContent, null));
                        item.put("context", objectMapper.convertValue(finalContext, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}));
                        item.put("contextHash", calculateContentHash(objectMapper.convertValue(finalContext, JsonNode.class).toString(), null));
                        if (currentModel != null) item.put("model", currentModel);
                        results.add(item);
                        logger.debug("Extracted enrichable item: {}", item);
                    }
                }

                // Always recurse into container nodes
                if (fieldValue.isObject() || fieldValue.isArray()) {
                    String newJsonPath = currentJsonPath.equals("$") ? "$." + fieldKey : currentJsonPath + "." + fieldKey;
                    findAndExtractRecursive(fieldValue, newJsonPath, currentEnvelope, currentFacets, results);
                }
            });

        } else if (currentNode.isArray()) {
            for (int i = 0; i < currentNode.size(); i++) {
                String newJsonPath = currentJsonPath + "[" + i + "]";
                findAndExtractRecursive(currentNode.get(i), newJsonPath, parentEnvelope, parentFacets, results);
            }
        }
    }

    /**
     * Cleanses embedded templating syntax, HTML, and extra whitespace.
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