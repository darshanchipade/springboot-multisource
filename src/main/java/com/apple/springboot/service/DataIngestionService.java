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

@Service
public class DataIngestionService {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionService.class);
    private static final Set<String> CONTENT_FIELD_KEYS = Set.of("copy", "disclaimer", "analytics");

    private final RawDataStoreRepository rawDataStoreRepository;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;
    private final ResourceLoader resourceLoader;
    private final String jsonFilePath;
    private final S3StorageService s3StorageService;
    private final String defaultS3BucketName;
    private final ContentHashRepository contentHashRepository;

    public DataIngestionService(RawDataStoreRepository rawDataStoreRepository,
                                CleansedDataStoreRepository cleansedDataStoreRepository,
                                ContentHashRepository contentHashRepository,
                                ObjectMapper objectMapper,
                                ResourceLoader resourceLoader,
                                @Value("${app.json.file.path}") String jsonFilePath,
                                S3StorageService s3StorageService,
                                @Value("${app.s3.bucket-name}") String defaultS3BucketName) {
        this.rawDataStoreRepository = rawDataStoreRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.contentHashRepository = contentHashRepository;
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
        logger.info("Starting ingestion for sourceIdentifier: {}", sourceIdentifier);

        if (jsonPayload == null || jsonPayload.trim().isEmpty()) {
            RawDataStore rawData = new RawDataStore();
            rawData.setSourceUri(sourceIdentifier);
            rawData.setReceivedAt(OffsetDateTime.now());
            rawData.setRawContentText(jsonPayload);
            rawData.setStatus("EMPTY_PAYLOAD");
            rawDataStoreRepository.save(rawData);
            return createAndSaveErrorCleansedDataStore(rawData, "SOURCE_EMPTY_PAYLOAD", "PayloadError: Received empty or null JSON payload.");
        }

        String newContentHash = calculateContentHash(jsonPayload, null);
        Optional<RawDataStore> latestVersionOpt = rawDataStoreRepository.findTopBySourceUriOrderByVersionDesc(sourceIdentifier);

        RawDataStore rawDataStore;
        if (latestVersionOpt.isPresent()) {
            rawDataStore = latestVersionOpt.get();
            if (Objects.equals(rawDataStore.getContentHash(), newContentHash)) {
                logger.info("Ingested content for sourceIdentifier '{}' has not changed. Skipping processing.", sourceIdentifier);
                return cleansedDataStoreRepository.findTopByRawDataIdOrderByCleansedAtDesc(rawDataStore.getId()).orElse(null);
            }
            logger.info("Found existing RawDataStore with ID {}. Content has changed, updating record.", rawDataStore.getId());
        } else {
            rawDataStore = new RawDataStore();
            rawDataStore.setSourceUri(sourceIdentifier);
            rawDataStore.setVersion(1);
            logger.info("No existing RawDataStore found for sourceIdentifier {}. Creating a new one.", sourceIdentifier);
        }

        rawDataStore.setReceivedAt(OffsetDateTime.now());
        rawDataStore.setRawContentText(jsonPayload);
        rawDataStore.setContentHash(newContentHash);
        rawDataStore.setStatus("RECEIVED");

        RawDataStore savedRawDataStore = rawDataStoreRepository.save(rawDataStore);
        return processLoadedContent(jsonPayload, savedRawDataStore);
    }
    @Transactional
    public CleansedDataStore ingestAndCleanseSingleFile() throws IOException {
        return ingestAndCleanseSingleFile(this.jsonFilePath);
    }

    @Transactional
    public CleansedDataStore ingestAndCleanseSingleFile(String identifier) throws IOException {
        logger.info("Starting ingestion for identifier: {}", identifier);
        String rawJsonContent;
        String sourceUriForDb = identifier;

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
            RawDataStore errorStore = new RawDataStore();
            errorStore.setSourceUri(sourceUriForDb);
            errorStore.setReceivedAt(OffsetDateTime.now());
            errorStore.setStatus("CONTENT_LOAD_ERROR");
            errorStore.setRawContentText("Failed to load content: " + e.getMessage());
            rawDataStoreRepository.save(errorStore);
            return createAndSaveErrorCleansedDataStore(errorStore, "CONTENT_LOAD_ERROR", "ContentLoadError: " + e.getMessage());
        }

        return ingestAndCleanseJsonPayload(rawJsonContent, sourceUriForDb);
    }

    private CleansedDataStore processLoadedContent(String rawJsonContent, RawDataStore associatedRawDataStore) {
        List<Map<String, Object>> allExtractedItems;
        try {
            JsonNode rootNode = objectMapper.readTree(rawJsonContent);
            allExtractedItems = new ArrayList<>();
            findAndExtractRecursive(rootNode, "$", new Envelope(), new Facets(), allExtractedItems);
        } catch (Exception e) {
            associatedRawDataStore.setStatus("EXTRACTION_ERROR");
            rawDataStoreRepository.save(associatedRawDataStore);
            return createAndSaveErrorCleansedDataStore(associatedRawDataStore, "EXTRACTION_FAILED", "ExtractionError: " + e.getMessage());
        }

        List<Map<String, Object>> itemsToProcess = new ArrayList<>();
        for (Map<String, Object> item : allExtractedItems) {
            String sourcePath = (String) item.get("sourcePath");
            String itemType = (String) item.get("itemType");
            String newContentHash = (String) item.get("contentHash");

            Optional<ContentHash> existingHashOpt = contentHashRepository.findBySourcePathAndItemType(sourcePath, itemType);

            if (existingHashOpt.isEmpty()) {
                logger.debug("No existing content hash found for item [{}::{}]. Marking as new.", sourcePath, itemType);
                itemsToProcess.add(item);
                contentHashRepository.save(new ContentHash(sourcePath, itemType, newContentHash, (String) item.get("contextHash")));
            } else {
                ContentHash existingHash = existingHashOpt.get();
                if (!Objects.equals(existingHash.getContentHash(), newContentHash)) {
                    logger.debug("Content hash changed for item [{}::{}]. Marking for update.", sourcePath, itemType);
                    itemsToProcess.add(item);
                    existingHash.setContentHash(newContentHash);
                    existingHash.setContextHash((String) item.get("contextHash"));
                    contentHashRepository.save(existingHash);
                } else {
                    logger.debug("Content hash for item [{}::{}] is unchanged. Skipping.", sourcePath, itemType);
                }
            }
        }

        if (itemsToProcess.isEmpty()) {
            logger.info("No new or updated content to process for raw_data_id: {}", associatedRawDataStore.getId());
            associatedRawDataStore.setStatus("PROCESSED_NO_CHANGES");
            rawDataStoreRepository.save(associatedRawDataStore);
            return cleansedDataStoreRepository.findTopByRawDataIdOrderByCleansedAtDesc(associatedRawDataStore.getId()).orElse(null);
        }

        CleansedDataStore cleansedDataStore = new CleansedDataStore();
        cleansedDataStore.setRawDataId(associatedRawDataStore.getId());
        cleansedDataStore.setSourceUri(associatedRawDataStore.getSourceUri());
        cleansedDataStore.setCleansedAt(OffsetDateTime.now());
        cleansedDataStore.setCleansedItems(itemsToProcess);
        cleansedDataStore.setVersion(associatedRawDataStore.getVersion());
        cleansedDataStore.setStatus("CLEANSED_PENDING_ENRICHMENT");

        associatedRawDataStore.setStatus("CLEANSING_COMPLETE");
        rawDataStoreRepository.save(associatedRawDataStore);

        return cleansedDataStoreRepository.save(cleansedDataStore);

    }

    private String calculateContentHash(String content, String context) {
        if (content == null || content.isEmpty()) return null;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(content.getBytes(StandardCharsets.UTF_8));
            if (context != null && !context.isEmpty()) {
                digest.update(context.getBytes(StandardCharsets.UTF_8));
            }
            byte[] encodedhash = digest.digest();
            return bytesToHex(encodedhash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private CleansedDataStore createAndSaveErrorCleansedDataStore(RawDataStore rawDataStore, String cleansedStatus, String errorMessage) {
        CleansedDataStore errorCleansedData = new CleansedDataStore();
        if (rawDataStore != null) {
            errorCleansedData.setRawDataId(rawDataStore.getId());
            errorCleansedData.setSourceUri(rawDataStore.getSourceUri());
        }
        errorCleansedData.setCleansedItems(Collections.emptyList());
        errorCleansedData.setStatus(cleansedStatus);
        errorCleansedData.setCleansingErrors(Map.of("error", errorMessage));
        errorCleansedData.setCleansedAt(OffsetDateTime.now());
        return cleansedDataStoreRepository.save(errorCleansedData);
    }

    private void findAndExtractRecursive(JsonNode currentNode, String currentJsonPath, Envelope parentEnvelope, Facets parentFacets, List<Map<String, Object>> results) {
        if (currentNode.isObject()) {
            Envelope currentEnvelope = new Envelope();
            currentEnvelope.setModel(currentNode.path("_model").asText(parentEnvelope.getModel()));
            currentEnvelope.setSourcePath(currentNode.path("_path").asText(parentEnvelope.getSourcePath()));

            Facets currentFacets = new Facets();
            currentFacets.putAll(parentFacets);
            currentNode.fields().forEachRemaining(entry -> {
                if (entry.getValue().isValueNode() && !entry.getKey().startsWith("_")) {
                    currentFacets.put(entry.getKey(), entry.getValue().asText());
                }
            });

            currentNode.fields().forEachRemaining(entry -> {
                String fieldKey = entry.getKey();
                JsonNode fieldValue = entry.getValue();

                if (CONTENT_FIELD_KEYS.contains(fieldKey) && fieldValue.isTextual()) {
                    String cleansedContent = cleanseCopyText(fieldValue.asText());
                    if (cleansedContent != null && !cleansedContent.isBlank()) {
                        EnrichmentContext finalContext = new EnrichmentContext(currentEnvelope, currentFacets);
                        Map<String, Object> item = new HashMap<>();
                        item.put("sourcePath", currentEnvelope.getSourcePath());
                        item.put("itemType", fieldKey);
                        item.put("cleansedContent", cleansedContent);
                        item.put("contentHash", calculateContentHash(cleansedContent, null));
                        try {
                            item.put("context", objectMapper.convertValue(finalContext, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}));
                            item.put("contextHash", calculateContentHash(objectMapper.writeValueAsString(finalContext), null));
                        } catch (JsonProcessingException e) {
                            logger.error("Failed to process context for hashing", e);
                        }
                        results.add(item);
                    }
                } else if (fieldValue.isObject() || fieldValue.isArray()) {
                    findAndExtractRecursive(fieldValue, currentJsonPath + "." + fieldKey, currentEnvelope, currentFacets, results);
                }
            });
        } else if (currentNode.isArray()) {
            for (int i = 0; i < currentNode.size(); i++) {
                findAndExtractRecursive(currentNode.get(i), currentJsonPath + "[" + i + "]", parentEnvelope, parentFacets, results);
            }
        }
    }
    private static String cleanseCopyText(String text) {
        if (text == null) return null;
        String cleansed = text.replaceAll("\\{%.*?%\\}", " ");
        cleansed = cleansed.replaceAll("<[^>]+?>", " ");
        cleansed = cleansed.replaceAll("\s+", " ").trim();
        return cleansed;
    }
}