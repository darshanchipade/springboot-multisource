package com.apple.springboot.service;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;


import java.util.*;

import java.util.Map;

@Service
public class BedrockEnrichmentService {

    private static final Logger logger = LoggerFactory.getLogger(BedrockEnrichmentService.class);
    private final BedrockRuntimeClient bedrockClient;
    private final ObjectMapper objectMapper;
    private final String bedrockModelId; // Store the model ID
    private final String bedrockRegion; // Store the region

    @Autowired
    public BedrockEnrichmentService(ObjectMapper objectMapper,
                                    @Value("${aws.region}") String region,
                                    @Value("${aws.bedrock.modelId}") String modelId) {
        this.objectMapper = objectMapper;
        this.bedrockRegion = region;     // Store the injected region
        this.bedrockModelId = modelId;  // Store the injected model ID

        if (region == null) {
            // This check is more for robustness, Spring should prevent null for @Value resolved params
            // unless the property itself is explicitly set to an empty value somehow and not caught by default.
            logger.error("AWS Region for Bedrock is null. Cannot initialize BedrockRuntimeClient.");
            throw new IllegalArgumentException("AWS Region for Bedrock must not be null.");
        }

//        this.bedrockClient = BedrockRuntimeClient.builder()
//                .region(Region.of(this.bedrockRegion)) // Use the stored region
//                .credentialsProvider(DefaultCredentialsProvider.create())
//                .build();

        this.bedrockClient = BedrockRuntimeClient.builder()
                .region(Region.of(this.bedrockRegion))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.defaultRetryPolicy())
                        .build())
                .build();
        logger.info("BedrockEnrichmentService initialized with region: {} and model ID: {}", this.bedrockRegion, this.bedrockModelId);
    }

    public String getConfiguredModelId() {
        return this.bedrockModelId;
    }

    public Map<String, Object> enrichText(String textToEnrich, String modelHint) {
        String effectiveModelId = this.bedrockModelId;;
        logger.info("Starting enrichment for text using model: {}. Text length: {}", effectiveModelId, textToEnrich.length());

        Map<String, Object> results = new HashMap<>();
        results.put("enrichedWithModel", effectiveModelId);

        try {
            String prompt = "Human: You are an AI assistant. Analyze the following text and provide these details in a single, valid JSON object with no extra commentary before or after the JSON:\n" +
                    "1. A concise summary (key: \"summary\").\n" +
                    "2. A list of up to 10 relevant keywords (key: \"keywords\", value should be a JSON array of strings).\n" +
                    "3. The overall sentiment (e.g., positive, negative, neutral) (key: \"sentiment\").\n" +
                    "4. A content classification or category (key: \"classification\").\n" +
                    "5. A list of up to 5 relevant tags (key: \"tags\", value should be a JSON array of strings).\n" +
                    "Text to analyze is below:\n" +
                    "<text>\n" + textToEnrich + "\n</text>\n\n" +
                    "Assistant: Here is the JSON output as requested:\n";

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

            logger.debug("Bedrock InvokeModel Request Body: {}", payloadJson);
            InvokeModelResponse response = bedrockClient.invokeModel(request);
            String responseBodyString = response.body().asUtf8String();
            logger.debug("Bedrock InvokeModel Response Body: {}", responseBodyString);

            JsonNode responseJson = objectMapper.readTree(responseBodyString);
            JsonNode contentBlock = responseJson.path("content");

            if (contentBlock.isArray() && contentBlock.size() > 0) {
                String textContent = contentBlock.get(0).path("text").asText("");
                if (textContent.startsWith("{") && textContent.endsWith("}")) {
                    try {
                        JsonNode extractedJson = objectMapper.readTree(textContent);
                        results.put("summary", extractedJson.path("summary").asText("Error parsing summary"));
                        results.put("keywords", jsonNodeToList(extractedJson.path("keywords")));
                        results.put("sentiment", extractedJson.path("sentiment").asText("Error parsing sentiment"));
                        results.put("classification", extractedJson.path("classification").asText("Error parsing classification"));
                        results.put("tags", jsonNodeToList(extractedJson.path("tags")));
                        logger.info("Successfully parsed enrichments from Bedrock response for model: {}", effectiveModelId);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to parse JSON content from Bedrock response: {}. Error: {}", textContent, e.getMessage());
                        results.put("error", "Failed to parse JSON from Bedrock response");
                        results.put("raw_bedrock_response", textContent);
                    }
                } else {
                    logger.error("Bedrock response content is not a JSON object: {}", textContent);
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
        } catch (JsonProcessingException e) {
            logger.error("JSON processing error during Bedrock request/response handling for model {}: {}", effectiveModelId, e.getMessage(), e);
            results.put("error", "JSON processing error: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error during Bedrock enrichment for model {}: {}", effectiveModelId, e.getMessage(), e);
            results.put("error", "Unexpected error during enrichment: " + e.getMessage());
        }
        return results;
    }

    private List<String> jsonNodeToList(JsonNode node) {
        if (node == null || node.isMissingNode() || !node.isArray()) {
            return Collections.emptyList();
        }
        List<String> list = new ArrayList<>();
        for (JsonNode element : node) {
            list.add(element.asText());
        }
        return list;
    }
}