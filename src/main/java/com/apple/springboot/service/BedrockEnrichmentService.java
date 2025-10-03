
package com.apple.springboot.service;

import com.apple.springboot.model.EnrichmentContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.io.IOException;
import java.util.*;
import java.util.Map;


@Service
public class BedrockEnrichmentService {

    private static final Logger logger = LoggerFactory.getLogger(BedrockEnrichmentService.class);
    private final BedrockRuntimeClient bedrockClient;
    private final ObjectMapper objectMapper;
    private final String bedrockModelId; // Store the model ID
    private final String bedrockRegion; // Store the region
    private final String embeddingModelId;

    @Autowired
    public BedrockEnrichmentService(ObjectMapper objectMapper,
                                    @Value("${aws.region}") String region,
                                    @Value("${aws.bedrock.modelId}") String modelId,
                                    @Value("${aws.bedrock.embeddingModelId}") String embeddingModelId) {
        this.objectMapper = objectMapper;
        this.bedrockRegion = region;     // Store the injected region
        this.bedrockModelId = modelId;  // Store the injected model ID
        this.embeddingModelId = embeddingModelId;

        if (region == null) {
            // This check is more for robustness, Spring should prevent null for @Value resolved params
            // unless the property itself is explicitly set to an empty value somehow and not caught by default.
            logger.error("AWS Region for Bedrock is null. Cannot initialize BedrockRuntimeClient.");
            throw new IllegalArgumentException("AWS Region for Bedrock must not be null.");
        }

        this.bedrockClient = BedrockRuntimeClient.builder()
                .region(Region.of(this.bedrockRegion))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        logger.info("BedrockEnrichmentService initialized with region: {} and model ID: {}", this.bedrockRegion, this.bedrockModelId);
    }

    public String getConfiguredModelId() {
        return this.bedrockModelId;
    }

    public float[] generateEmbedding(String text) throws IOException {
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

        InvokeModelResponse response = bedrockClient.invokeModel(request);
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



        // New, more detailed prompt template
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


    @RateLimiter(name = "bedrock")
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
            payload.put("max_tokens", 4096);
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
            InvokeModelResponse response = bedrockClient.invokeModel(request);
            String responseBodyString = response.body().asUtf8String();
            logger.debug("Bedrock InvokeModel Response Body for path {}: {}", sourcePath, responseBodyString);

            JsonNode responseJson = objectMapper.readTree(responseBodyString);
            JsonNode contentBlock = responseJson.path("content");

            if (contentBlock.isArray() && contentBlock.size() > 0) {
                String textContent = contentBlock.get(0).path("text").asText("").trim();

                // Check for and strip Markdown JSON code fences
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

    private List<String> jsonNodeToList(JsonNode node) {
        List<String> list = new ArrayList<>();
        if (node != null && node.isArray()) {
            for (JsonNode element : node) {
                list.add(element.asText());
            }
        }
        return list;
    }

    /**
     * Enriches a batch of items. Note that the underlying Bedrock InvokeModel API for Anthropic Claude
     * does not support batching multiple different prompts in a single synchronous call. This method
     * therefore iterates over the items and calls the enrichment service for each one.
     * Rate limiting is handled by the Resilience4j RateLimiter aspect and manual delays in the calling service.
     */
    public Map<String, Map<String, Object>> enrichBatch(List<CleansedItemDetail> batch) {
        Map<String, Map<String, Object>> batchResults = new HashMap<>();
        for (CleansedItemDetail item : batch) {
            String fullContextId = item.sourcePath + "::" + item.originalFieldName;
            try {
                // Convert CleansedItemDetail to the required JsonNode and EnrichmentContext for enrichItem
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

    /**
     * Generates embeddings for a batch of texts using a single call to the Amazon Titan Embedding model.
     * The Titan model's InvokeModel API supports receiving an array of strings in the 'inputText' field.
     * @param texts A list of strings to be embedded.
     * @return A list of float arrays, where each array is the embedding for the corresponding text.
     * @throws IOException if there is an error during JSON processing or the API call.
     */
    public List<float[]> generateEmbeddingsInBatch(List<String> texts) throws IOException {
        if (texts == null || texts.isEmpty()) {
            return Collections.emptyList();
        }

        List<float[]> allEmbeddings = new ArrayList<>();
        ObjectNode payload = objectMapper.createObjectNode();
        // The Titan embedding model expects a JSON array for batch, and a single string for single item.
        // To keep it simple, we'll handle the single item case by wrapping it in a list and processing as a batch.
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
            InvokeModelResponse response = bedrockClient.invokeModel(request);
            JsonNode responseJson = objectMapper.readTree(response.body().asUtf8String());
            JsonNode embeddingsNode = responseJson.get("embedding");

            if (embeddingsNode != null && embeddingsNode.isArray()) {
                // The response for a batch request is an array of embedding arrays.
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
}
