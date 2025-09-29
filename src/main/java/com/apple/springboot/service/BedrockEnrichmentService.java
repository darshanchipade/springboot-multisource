package com.apple.springboot.service;

import com.apple.springboot.model.EnrichmentContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.io.IOException;
import java.util.*;

@Service
public class BedrockEnrichmentService {

    private static final Logger logger = LoggerFactory.getLogger(BedrockEnrichmentService.class);
    private final BedrockRuntimeClient bedrockClient;
    private final ObjectMapper objectMapper;
    private final String bedrockModelId;
    private final String bedrockRegion;
    private final String embeddingModelId;

    public BedrockEnrichmentService(ObjectMapper objectMapper,
                                    @Value("${aws.region}") String region,
                                    @Value("${aws.bedrock.modelId}") String modelId,
                                    @Value("${aws.bedrock.embeddingModelId}") String embeddingModelId) {
        this.objectMapper = objectMapper;
        this.bedrockRegion = region;
        this.bedrockModelId = modelId;
        this.embeddingModelId = embeddingModelId;

        if (region == null) {
            logger.error("AWS Region for Bedrock is null.");
            throw new IllegalArgumentException("AWS Region for Bedrock must not be null.");
        }

        this.bedrockClient = BedrockRuntimeClient.builder()
                .region(Region.of(this.bedrockRegion))
                .build();
        logger.info("BedrockEnrichmentService initialized with region: {} and model ID: {}", this.bedrockRegion, this.bedrockModelId);
    }

    public String getConfiguredModelId() {
        return this.bedrockModelId;
    }

    @RateLimiter(name = "bedrockEmbedder")
    @Retry(name = "bedrockEmbedder")
    public List<float[]> generateEmbeddingsInBatch(List<String> texts) throws IOException {
        if (texts == null || texts.isEmpty()) {
            logger.warn("No texts provided for embedding");
            return Collections.emptyList();
        }

        List<float[]> results = new ArrayList<>(texts.size());
        for (String text : texts) {
            try {
                results.add(generateEmbeddingSafe(text));
            } catch (IOException e) {
                logger.warn("Failed to generate embedding for text (length {}): {}", text != null ? text.length() : 0, e.getMessage());
                results.add(new float[0]);
            }
        }
        logger.info("Generated {} embeddings for batch of {} texts", results.size(), texts.size());
        return results;
    }

    private List<float[]> invokeTitanEmbeddings(List<String> texts) throws IOException {
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

        logger.debug("Sending Titan batch request for {} texts at {}", texts.size(), System.currentTimeMillis());
        InvokeModelResponse response = bedrockClient.invokeModel(request);
        JsonNode root = objectMapper.readTree(response.body().asUtf8String());
        List<float[]> results = new ArrayList<>(texts.size());

        if (root.has("embeddings") && root.get("embeddings").isArray()) {
            for (JsonNode item : root.get("embeddings")) {
                JsonNode emb = item.get("embedding");
                if (emb == null || !emb.isArray()) {
                    throw new IOException("Titan batch response missing 'embedding' array.");
                }
                float[] vec = new float[emb.size()];
                for (int i = 0; i < emb.size(); i++) {
                    vec[i] = emb.get(i).floatValue();
                }
                results.add(vec);
            }
            logger.info("Received {} embeddings for batch at {}", results.size(), System.currentTimeMillis());
            return results;
        }

        if (root.has("embedding") && root.get("embedding").isArray() && texts.size() == 1) {
            JsonNode emb = root.get("embedding");
            float[] vec = new float[emb.size()];
            for (int i = 0; i < emb.size(); i++) vec[i] = emb.get(i).floatValue();
            results.add(vec);
            return results;
        }

        throw new IOException("Unexpected Titan embeddings response shape: " + root.toString());
    }

    @RateLimiter(name = "bedrock")
    @Retry(name = "bedrockRetry")
    public Map<String, Map<String, Object>> enrichBatch(List<CleansedItemDetail> batch) {
        if (batch == null || batch.isEmpty()) {
            logger.warn("No items provided for batch enrichment");
            return new HashMap<>();
        }

        Map<String, Map<String, Object>> results = new HashMap<>();
        try {
            // Prepare prompt
            String prompt = """
    You are a content enrichment system. For each item in the provided JSON list, generate enrichments with the following structure:
    {
      "id": "<item_id>",
      "standardEnrichments": {
        "summary": "<brief summary of the content>",
        "keywords": ["<keyword1>", "<keyword2>", ...],
        "sentiment": "<positive|neutral|negative>",
        "classification": "<category or type>",
        "tags": ["<tag1>", "<tag2>", ...]
      }
    }
    Input items: %s
    Return a JSON object with an "items" array containing one entry per input item, matching each item by its "id". Ensure all input items are included in the response, even if enrichment fails (use empty standardEnrichments if needed).
    Example response:
    {
      "items": [
        {"id": "item1::field1", "standardEnrichments": {...}},
        {"id": "item2::field2", "standardEnrichments": {...}}
      ]
    }
    Output only the JSON object, with no additional text or commentary.
    """.formatted(objectMapper.writeValueAsString(
                    batch.stream().map(item -> Map.of(
                            "id", item.sourcePath + "::" + item.originalFieldName,
                            "content", item.cleansedContent
                    )).toList()
            ));

            // Prepare payload for Claude 3.5 Sonnet
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("anthropic_version", "bedrock-2023-05-31");
            payload.put("max_tokens", 2000);  // Use max_tokens for Claude 3 models
            payload.put("temperature", 0.5);
            ArrayNode messages = objectMapper.createArrayNode();
            ObjectNode message = objectMapper.createObjectNode();
            message.put("role", "user");
            message.put("content", prompt);
            messages.add(message);
            payload.set("messages", messages);
            String payloadJson = objectMapper.writeValueAsString(payload);
            SdkBytes body = SdkBytes.fromUtf8String(payloadJson);

            // Invoke Bedrock
            InvokeModelRequest request = InvokeModelRequest.builder()
                    .modelId(bedrockModelId)
                    .contentType("application/json")
                    .accept("application/json")
                    .body(body)
                    .build();

            logger.debug("Bedrock batch request for {} items at {}", batch.size(), System.currentTimeMillis());
            InvokeModelResponse response = bedrockClient.invokeModel(request);
            String responseBody = response.body().asUtf8String();
            logger.debug("Bedrock batch response at {}: {}", System.currentTimeMillis(), responseBody);

            // Parse Claude response
            Map<String, Object> responseJson = objectMapper.readValue(responseBody, Map.class);
            List<Map<String, Object>> content = (List<Map<String, Object>>) responseJson.get("content");
            if (content == null || content.isEmpty()) {
                logger.warn("No content in Bedrock response");
                batch.forEach(item -> results.put(
                        item.sourcePath + "::" + item.originalFieldName,
                        Map.of("error", "No content in Bedrock response")
                ));
                return results;
            }

            // Extract JSON from content[0].text, removing any non-JSON prefix
            Map<String, Object> contentMap = content.get(0);
            String text = (String) contentMap.get("text");
            // Find the start of the JSON object
            int jsonStart = text.indexOf("{");
            if (jsonStart == -1) {
                logger.warn("No valid JSON found in content text: {}", text);
                batch.forEach(item -> results.put(
                        item.sourcePath + "::" + item.originalFieldName,
                        Map.of("error", "No valid JSON in content text")
                ));
                return results;
            }
            String jsonText = text.substring(jsonStart);
            Map<String, Object> parsedItems = objectMapper.readValue(jsonText, Map.class);
            List<Map<String, Object>> items = (List<Map<String, Object>>) parsedItems.get("items");

            for (Map<String, Object> item : items) {
                String id = (String) item.get("id");
                @SuppressWarnings("unchecked")
                Map<String, Object> standardEnrichments = (Map<String, Object>) item.get("standardEnrichments");
                if (standardEnrichments == null) {
                    logger.warn("Missing standardEnrichments for item ID: {}", id);
                    results.put(id, Map.of("error", "Missing standardEnrichments"));
                    continue;
                }
                Map<String, Object> result = new HashMap<>();
                result.put("standardEnrichments", standardEnrichments);
                result.put("enrichedWithModel", bedrockModelId);
                results.put(id, result);
            }

            // Handle missing items
            for (CleansedItemDetail item : batch) {
                String fullContextId = item.sourcePath + "::" + item.originalFieldName;
                if (!results.containsKey(fullContextId)) {
                    logger.warn("No enrichment result for item ID: {}", fullContextId);
                    results.put(fullContextId, Map.of("error", "Item not found in Bedrock response"));
                }
            }

            return results;
        } catch (Exception e) {
            logger.error("Failed to enrich batch: {}", e.getMessage(), e);
            batch.forEach(item -> results.put(
                    item.sourcePath + "::" + item.originalFieldName,
                    Map.of("error", e.getMessage())
            ));
            return results;
        }
    }
    @RateLimiter(name = "bedrock")
    @Retry(name = "bedrockRetry")
    public Map<String, Object> enrichItem(JsonNode itemContent, EnrichmentContext context) {
        String effectiveModelId = this.bedrockModelId;
        String sourcePath = (context != null && context.getEnvelope() != null) ? context.getEnvelope().getSourcePath() : "Unknown";
        logger.info("Starting enrichment for item using model: {}. Item path: {} at {}", effectiveModelId, sourcePath, System.currentTimeMillis());

        Map<String, Object> results = new HashMap<>();
        results.put("enrichedWithModel", effectiveModelId);

        try {
            String textToEnrich = itemContent.path("cleansedContent").asText("");
            float[] embedding = generateEmbeddingSafe(textToEnrich);
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
                if (textContent.startsWith("```json")) {
                    textContent = textContent.substring(7).trim();
                    if (textContent.endsWith("```")) {
                        textContent = textContent.substring(0, textContent.length() - 3).trim();
                    }
                }

                if (textContent.startsWith("{") && textContent.endsWith("}")) {
                    try {
                        Map<String, Object> aiResults = objectMapper.readValue(textContent, new com.fasterxml.jackson.core.type.TypeReference<>() {
                        });
                        aiResults.put("enrichedWithModel", effectiveModelId);
                        return aiResults;
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to parse JSON content from Bedrock response: {}. Error: {}", textContent, e.getMessage());
                        results.put("error", "Failed to parse JSON from Bedrock response");
                        results.put("raw_bedrock_response", textContent);
                        return results;
                    }
                } else {
                    logger.error("Bedrock response content is not a JSON object: {}", textContent);
                    results.put("error", "Bedrock response content is not a JSON object");
                    results.put("raw_bedrock_response", textContent);
                }
            } else {
                logger.error("Bedrock response does not contain expected content block.");
                results.put("error", "Bedrock response structure unexpected");
                results.put("raw_bedrock_response", responseBodyString);
            }
        } catch (BedrockRuntimeException e) {
            logger.error("Bedrock API error during enrichment for model {}: {} at {}", effectiveModelId, e.awsErrorDetails().errorMessage(), System.currentTimeMillis(), e);
            results.put("error", "Bedrock API error: " + e.awsErrorDetails().errorMessage());
            results.put("aws_error_code", e.awsErrorDetails().errorCode());
            return results;
        } catch (JsonProcessingException e) {
            logger.error("JSON processing error during Bedrock request/response handling for model {}: {} at {}", effectiveModelId, e.getMessage(), System.currentTimeMillis(), e);
            results.put("error", "JSON processing error: " + e.getMessage());
            return results;
        } catch (Exception e) {
            logger.error("Unexpected error during Bedrock enrichment for model {}: {} at {}", effectiveModelId, e.getMessage(), System.currentTimeMillis(), e);
            results.put("error", "Unexpected error during enrichment: " + e.getMessage());
            return results;
        }
        return results;
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

    @RateLimiter(name = "bedrockEmbedder")
    @Retry(name = "bedrockEmbedder")
    public float[] generateEmbeddingSafe(String text) throws IOException {
        if (text == null || text.isEmpty()) {
            logger.warn("Skipping embedding for empty text");
            return new float[0];
        }
        return generateEmbedding(text);
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

        logger.debug("Sending Titan embedding request for text (length {}) at {}", text.length(), System.currentTimeMillis());
        InvokeModelResponse response = bedrockClient.invokeModel(request);
        JsonNode responseJson = objectMapper.readTree(response.body().asUtf8String());
        JsonNode embeddingNode = responseJson.get("embedding");
        if (embeddingNode == null || !embeddingNode.isArray()) {
            throw new IOException("Invalid embedding response: " + responseJson.toString());
        }
        float[] embedding = new float[embeddingNode.size()];
        for (int i = 0; i < embeddingNode.size(); i++) {
            embedding[i] = embeddingNode.get(i).floatValue();
        }
        return embedding;
    }
}