package com.apple.springboot.service;

import com.apple.springboot.model.EnrichmentContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class BedrockEnrichmentService {

    private static final Logger logger = LoggerFactory.getLogger(BedrockEnrichmentService.class);
    private final BedrockRuntimeClient bedrockClient;
    private final ObjectMapper objectMapper;
    private final String bedrockModelId;
    private final String embeddingModelId;
    private final RateLimiter chatRateLimiter;
    private final RateLimiter embedRateLimiter;
    private final int bedrockMaxTokens;

    @Autowired
    public BedrockEnrichmentService(
            ObjectMapper objectMapper,
            BedrockRuntimeClient bedrockRuntimeClient,
            @Value("${aws.bedrock.modelId}") String modelId,
            @Value("${aws.bedrock.embeddingModelId}") String embeddingModelId,
            @Value("${app.bedrock.maxTokens:1024}") int maxTokens,
            RateLimiter bedrockChatRateLimiter,
            RateLimiter bedrockEmbedRateLimiter
    ) {
        this.objectMapper = objectMapper;
        this.bedrockClient = bedrockRuntimeClient;
        this.bedrockModelId = modelId;
        this.embeddingModelId = embeddingModelId;
        this.chatRateLimiter = bedrockChatRateLimiter;
        this.embedRateLimiter = bedrockEmbedRateLimiter;
        this.bedrockMaxTokens = Math.max(128, maxTokens);
        logger.info("BedrockEnrichmentService initialized with model ID: {} and embed model: {}", this.bedrockModelId, this.embeddingModelId);
    }

    public String getConfiguredModelId() {
        return this.bedrockModelId;
    }

    public float[] generateEmbedding(String text) throws IOException {
        // Rate-limit embedding calls independently of chat calls
        if (embedRateLimiter != null) {
            embedRateLimiter.acquire();
        }
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("inputText", text);

        String payloadJson = objectMapper.writeValueAsString(payload);
        SdkBytes body = SdkBytes.fromUtf8String(payloadJson);

        InvokeModelRequest request = InvokeModelRequest.builder()
                .modelId(embeddingModelId)
                .contentType("application/json")
                .accept("application/json")
                .body(body)
                .build();

        InvokeModelResponse response = invokeWithRetry(request, true);
        JsonNode responseJson = objectMapper.readTree(response.body().asUtf8String());
        JsonNode embeddingNode = responseJson.get("embedding");
        float[] embedding = new float[embeddingNode.size()];
        for (int i = 0; i < embeddingNode.size(); i++) {
            embedding[i] = embeddingNode.get(i).floatValue();
        }
        return embedding;
    }

    private String createEnrichmentPrompt(JsonNode itemContent, EnrichmentContext context) throws JsonProcessingException {
        String cleansedContent = itemContent.path("cleansedContent").asText("");
        String contextJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(context);

        String promptTemplate =
                "Human: You are an expert content analyst AI. Your task is to analyze a piece of text and provide a set of standard content enrichments. " +
                        "You will be given the text itself in a <content> tag, and a rich JSON object providing the context of where this content lives in a <context> tag.\n\n" +
                        "Use the metadata in the <context> object, such as the 'pathHierarchy' and `facets`, to generate more accurate and relevant enrichments.\n\n" +
                        "Please provide a single, valid JSON object as your response with no extra commentary. " +
                        "The response JSON should have one top-level key: \"standardEnrichments\".\n\n" +
                        "The `standardEnrichments` object must contain the following keys:\n" +
                        "- 'summary': A concise summary of the content.\n" +
                        "- 'keywords': A JSON array of up to 10 relevant keywords. Keywords should be lowercase.\n" +
                        "- 'sentiment': The overall sentiment (choose one: positive, negative, neutral).\n" +
                        "- 'classification': A general content category (e.g., 'product description', 'legal disclaimer', 'promotional heading').\n" +
                        "- 'tags': A JSON array of up to 5 relevant tags that can be used for filtering or grouping.\n\n" +
                        "<content>\n%s\n</content>\n\n" +
                        "<context>\n%s\n</context>\n\n" +
                        "Assistant: Here is the single, valid JSON object with the requested enrichments:\n";

        return String.format(promptTemplate, cleansedContent, contextJson);
    }

    public Map<String, Object> enrichItem(JsonNode itemContent, EnrichmentContext context) {
        String effectiveModelId = this.bedrockModelId;
        String sourcePath = (context != null && context.getEnvelope() != null) ? context.getEnvelope().getSourcePath() : "Unknown";
        logger.info("Starting enrichment for item using model: {}. Item path: {}", effectiveModelId, sourcePath);

        Map<String, Object> results = new HashMap<>();
        results.put("enrichedWithModel", effectiveModelId);

        try {
            String textToEnrich = itemContent.path("cleansedContent").asText("");
            float[] embedding = generateEmbedding(textToEnrich);
            results.put("vector", embedding);
            String prompt = createEnrichmentPrompt(itemContent, context);

            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("anthropic_version", "bedrock-2023-05-31");
            payload.put("max_tokens", bedrockMaxTokens);
            List<ObjectNode> messages = new ArrayList<>();
            ObjectNode userMessage = objectMapper.createObjectNode();
            userMessage.put("role", "user");
            userMessage.put("content", prompt);
            messages.add(userMessage);
            payload.set("messages", objectMapper.valueToTree(messages));

            String payloadJson = objectMapper.writeValueAsString(payload);
            SdkBytes body = SdkBytes.fromUtf8String(payloadJson);

            InvokeModelRequest request = InvokeModelRequest.builder()
                    .modelId(bedrockModelId)
                    .contentType("application/json")
                    .accept("application/json")
                    .body(body)
                    .build();

            logger.debug("Bedrock InvokeModel Request for path {}: {}", sourcePath, payloadJson);
            // Rate-limit chat/completion separately
            if (chatRateLimiter != null) {
                chatRateLimiter.acquire();
            }
            InvokeModelResponse response = invokeWithRetry(request, false);
            String responseBodyString = response.body().asUtf8String();
            logger.debug("Bedrock InvokeModel Response Body for path {}: {}", sourcePath, responseBodyString);

            JsonNode responseJson = objectMapper.readTree(responseBodyString);
            JsonNode contentBlock = responseJson.path("content");

            if (contentBlock.isArray() && contentBlock.size() > 0) {
                String textContent = contentBlock.get(0).path("text").asText("").trim();

                if (textContent.startsWith("```json")) {
                    textContent = textContent.substring(7).trim();
                    if (textContent.endsWith("```")) {
                        textContent = textContent.substring(0, textContent.length() - 3).trim();
                    }
                }

                if (textContent.startsWith("{") && textContent.endsWith("}")) {
                    try {
                        Map<String, Object> aiResults = objectMapper.readValue(textContent, new TypeReference<>() {});
                        aiResults.put("enrichedWithModel", effectiveModelId);
                        return aiResults;
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to parse JSON content from Bedrock response: {}. Error: {}", textContent, e.getMessage());
                        results.put("error", "Failed to parse JSON from Bedrock response");
                        results.put("raw_bedrock_response", textContent);
                        return results;
                    }
                } else {
                    logger.error("Bedrock response content is not a JSON object after stripping fences: {}", textContent);
                    results.put("error", "Bedrock response content is not a JSON object");
                    results.put("raw_bedrock_response", textContent);
                }
            } else {
                logger.error("Bedrock response does not contain expected content block or content is not an array.");
                results.put("error", "Bedrock response structure unexpected");
                results.put("raw_bedrock_response", responseBodyString);
            }
        } catch (ThrottledException te) {
            // Surface throttling so caller can retry via SQS visibility timeout
            throw te;
        } catch (BedrockRuntimeException e) {
            logger.error("Bedrock API error during enrichment for model {}: {}", effectiveModelId, e.awsErrorDetails().errorMessage(), e);
            results.put("error", "Bedrock API error: " + e.awsErrorDetails().errorMessage());
            results.put("aws_error_code", e.awsErrorDetails().errorCode());
            return results;
        } catch (JsonProcessingException e) {
            logger.error("JSON processing error during Bedrock request/response handling for model {}: {}", effectiveModelId, e.getMessage(), e);
            results.put("error", "JSON processing error: " + e.getMessage());
            return results;
        } catch (Exception e) {
            logger.error("Unexpected error during Bedrock enrichment for model {}: {}", effectiveModelId, e.getMessage(), e);
            results.put("error", "Unexpected error during enrichment: " + e.getMessage());
            return results;
        }
        return results;
    }

    public Map<String, Map<String, Object>> enrichBatch(List<CleansedItemDetail> batch) {
        Map<String, Map<String, Object>> batchResults = new HashMap<>();
        for (CleansedItemDetail item : batch) {
            String fullContextId = item.sourcePath + "::" + item.originalFieldName;
            try {
                JsonNode itemContent = objectMapper.createObjectNode().put("cleansedContent", item.cleansedContent);
                Map<String, Object> result = enrichItem(itemContent, item.context);
                batchResults.put(fullContextId, result);
            } catch (Exception e) {
                logger.error("Error enriching item in batch: {}. Error: {}", fullContextId, e.getMessage(), e);
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("error", "Failed to process item in batch: " + e.getMessage());
                batchResults.put(fullContextId, errorResult);
            }
        }
        return batchResults;
    }

    public List<float[]> generateEmbeddingsInBatch(List<String> texts) throws IOException {
        if (texts == null || texts.isEmpty()) {
            return Collections.emptyList();
        }

        List<float[]> allEmbeddings = new ArrayList<>();
        ObjectNode payload = objectMapper.createObjectNode();
        payload.set("inputText", objectMapper.valueToTree(texts));

        String payloadJson = objectMapper.writeValueAsString(payload);
        SdkBytes body = SdkBytes.fromUtf8String(payloadJson);

        InvokeModelRequest request = InvokeModelRequest.builder()
                .modelId(embeddingModelId)
                .contentType("application/json")
                .accept("application/json")
                .body(body)
                .build();

        try {
            if (embedRateLimiter != null) {
                embedRateLimiter.acquire();
            }
            InvokeModelResponse response = invokeWithRetry(request, true);
            JsonNode responseJson = objectMapper.readTree(response.body().asUtf8String());
            JsonNode embeddingsNode = responseJson.get("embedding");

            if (embeddingsNode != null && embeddingsNode.isArray()) {
                for (JsonNode embeddingArray : embeddingsNode) {
                    float[] embedding = objectMapper.convertValue(embeddingArray, float[].class);
                    allEmbeddings.add(embedding);
                }
            }
        } catch (BedrockRuntimeException e) {
            logger.error("Bedrock API error during batch embedding: {}", e.awsErrorDetails().errorMessage(), e);
            throw new IOException("Bedrock API error during batch embedding.", e);
        }
        return allEmbeddings;
    }

    private InvokeModelResponse invokeWithRetry(InvokeModelRequest request, boolean isEmbedding) {
        int maxAttempts = 6;
        long baseBackoffMs = isEmbedding ? 400L : 800L;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return bedrockClient.invokeModel(request);
            } catch (BedrockRuntimeException e) {
                String errorCode = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null;
                int statusCode = e.statusCode();
                boolean throttled = statusCode == 429 || "ThrottlingException".equalsIgnoreCase(errorCode)
                        || "TooManyRequestsException".equalsIgnoreCase(errorCode)
                        || "ProvisionedThroughputExceededException".equalsIgnoreCase(errorCode);
                if (!throttled || attempt == maxAttempts) {
                    if (throttled) {
                        throw new ThrottledException("Bedrock throttling after retries", e);
                    }
                    throw e;
                }
                long jitter = ThreadLocalRandom.current().nextLong(50, 200);
                long sleepMs = (long) Math.min(10_000, baseBackoffMs * Math.pow(2, attempt - 1) + jitter);
                logger.warn("Bedrock throttled (attempt {}/{}). Backing off for {} ms. Error: {}",
                        attempt, maxAttempts, sleepMs, e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage());
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during backoff", ie);
                }
            }
        }
        throw new RuntimeException("Unreachable");
    }
}