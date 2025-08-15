package com.apple.springboot.service;

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

        this.bedrockClient = BedrockRuntimeClient.builder()
                .region(Region.of(this.bedrockRegion))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        logger.info("BedrockEnrichmentService initialized with region: {} and model ID: {}", this.bedrockRegion, this.bedrockModelId);
    }

    public String getConfiguredModelId() {
        return this.bedrockModelId;
    }

    private String createEnrichmentPrompt(JsonNode itemToEnrich) {
        String cleansedContent = itemToEnrich.path("cleansedContent").asText("");
        String sourcePath = itemToEnrich.path("sourcePath").asText("");

        // Basic prompt template. This can be externalized or made more complex.
        String promptTemplate =
                "Human: You are an expert content analyst AI. Your task is to analyze a JSON object representing a piece of content and enrich it. Please provide a single, valid JSON object as your response with no extra commentary.\n" +
                        "The response JSON should have two top-level keys: \"standardEnrichments\" and \"context\".\n" +
                        "\n" +
                        "1.  **standardEnrichments**: This object should contain the following based on the <cleansedContent>:\n" +
                        "    -   'summary': A concise summary.\n" +
                        "    -   'keywords': A JSON array of up to 10 relevant keywords.\n" +
                        "    -   'sentiment': The overall sentiment (e.g., positive, negative, neutral).\n" +
                        "    -   'classification': A general content category.\n" +
                        "    -   'tags': A JSON array of up to 5 relevant tags.\n" +
                        "\n" +
                        "2.  **context**: This object must describe the content's placement and origin, derived *from the sourcePath and model fields*.\n" +
                        "    -   'fullContextId': Generate a hierarchical ID by taking the last 3 non-uuid segments of the 'sourcePath', separated by colons. For example, if path is '/a/b/c/d/e', the ID should be 'c:d:e'.\n" +
                        "    -   'sourcePath': The original 'sourcePath' of the item.\n" +
                        "    -   'eventType': If the 'sourcePath' or other metadata suggests a holiday or event (e.g., 'mothers-day', 'valentines-day'), identify it here. Otherwise, null.\n" +
                        "    -   'provenance': An object containing metadata about this enrichment process. It must include:\n" +
                        "        -   'modelId': Use the value \"%s\".\n" +
                        "        -   'promptId': Use the static value \"content-enrichment-v2\".\n\n" +
                        "Here is the JSON object to analyze:\n" +
                        "<item_json>\n%s\n</item_json>\n\n" +
                        "Assistant: Here is the single, valid JSON object with the requested enrichments:\n";

        return String.format(promptTemplate, this.bedrockModelId, itemToEnrich.toString());

    }

    public Map<String, Object> enrichItem(JsonNode itemToEnrich) {
        String effectiveModelId = this.bedrockModelId;
        logger.info("Starting enrichment for item using model: {}. Item path: {}", effectiveModelId, itemToEnrich.path("sourcePath").asText());

        Map<String, Object> results = new HashMap<>();
        results.put("enrichedWithModel", effectiveModelId);

        int maxRetries = 5;
        int retryCount = 0;
        long backoff = 1000;

        while (retryCount < maxRetries) {
            try {
                String prompt = createEnrichmentPrompt(itemToEnrich);

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

                logger.debug("Bedrock InvokeModel Request for path {}: {}", itemToEnrich.path("sourcePath").asText(), payloadJson);
                InvokeModelResponse response = bedrockClient.invokeModel(request);
                String responseBodyString = response.body().asUtf8String();
                logger.debug("Bedrock InvokeModel Response Body for path {}: {}", itemToEnrich.path("sourcePath").asText(), responseBodyString);

                JsonNode responseJson = objectMapper.readTree(responseBodyString);
                JsonNode contentBlock = responseJson.path("content");

                if (contentBlock.isArray() && contentBlock.size() > 0) {
                    String textContent = contentBlock.get(0).path("text").asText("");
                    if (textContent.startsWith("{") && textContent.endsWith("}")) {
                        try {
                            // The entire response is the result map
                            return objectMapper.readValue(textContent, new TypeReference<Map<String, Object>>() {});
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
                    logger.error("Bedrock response does not contain expected content block or content is not an array.");
                    results.put("error", "Bedrock response structure unexpected");
                    results.put("raw_bedrock_response", responseBodyString);
                }
            } catch (BedrockRuntimeException e) {
                if (e.awsErrorDetails().errorCode().equals("ThrottlingException")) {
                    retryCount++;
                    if (retryCount >= maxRetries) {
                        logger.error("Bedrock API error during enrichment for model {}: {}", effectiveModelId, e.awsErrorDetails().errorMessage(), e);
                        results.put("error", "Bedrock API error: " + e.awsErrorDetails().errorMessage());
                        results.put("aws_error_code", e.awsErrorDetails().errorCode());
                        return results;
                    }
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    backoff *= 2;
                } else {
                    logger.error("Bedrock API error during enrichment for model {}: {}", effectiveModelId, e.awsErrorDetails().errorMessage(), e);
                    results.put("error", "Bedrock API error: " + e.awsErrorDetails().errorMessage());
                    results.put("aws_error_code", e.awsErrorDetails().errorCode());
                    return results;
                }
            } catch (JsonProcessingException e) {
                logger.error("JSON processing error during Bedrock request/response handling for model {}: {}", effectiveModelId, e.getMessage(), e);
                results.put("error", "JSON processing error: " + e.getMessage());
                return results;
            } catch (Exception e) {
                logger.error("Unexpected error during Bedrock enrichment for model {}: {}", effectiveModelId, e.getMessage(), e);
                results.put("error", "Unexpected error during enrichment: " + e.getMessage());
                return results;
            }
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
}