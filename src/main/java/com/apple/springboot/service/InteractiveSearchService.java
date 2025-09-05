package com.apple.springboot.service;

import com.apple.springboot.model.ChatMessage;
import com.apple.springboot.model.ContentChunk;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


@Service
public class InteractiveSearchService {

    private static final Logger logger = LoggerFactory.getLogger(InteractiveSearchService.class);

    private final BedrockRuntimeClient bedrockClient;
    private final ObjectMapper objectMapper;
    private final VectorSearchService vectorSearchService;
    private final String bedrockModelId;
    private final String interactiveSearchPrompt;

    @Autowired
    public InteractiveSearchService(BedrockRuntimeClient bedrockClient, ObjectMapper objectMapper, VectorSearchService vectorSearchService,
                                    @Value("${aws.bedrock.modelId}") String bedrockModelId,
                                    ResourceLoader resourceLoader) throws IOException {
        this.bedrockClient = bedrockClient;
        this.objectMapper = objectMapper;
        this.vectorSearchService = vectorSearchService;
        this.bedrockModelId = bedrockModelId;

        Resource resource = resourceLoader.getResource("classpath:prompts/interactive_search_prompt.txt");
        this.interactiveSearchPrompt = new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    public ChatMessage processMessage(ChatMessage userMessage, List<ChatMessage> conversationHistory) {
        try {
            String bedrockResponse = invokeBedrock(userMessage, conversationHistory);
            JsonNode responseJson = extractJsonFromResponse(bedrockResponse);

            if (responseJson != null) {
                List<ContentChunk> searchResults = performSearch(responseJson);
                String formattedResults = formatResults(searchResults);
                return new ChatMessage(formattedResults, ChatMessage.Sender.BOT, userMessage.getConversationId(), null);
            } else {
                // The response was not JSON, so return it as a text message
                return new ChatMessage(bedrockResponse, ChatMessage.Sender.BOT, userMessage.getConversationId(), null);
            }

        } catch (IOException e) {
            logger.error("Error processing chat message", e);
            return new ChatMessage("Sorry, I encountered an error. Please try again.", ChatMessage.Sender.BOT, userMessage.getConversationId(), null);
        }
    }

    private String invokeBedrock(ChatMessage userMessage, List<ChatMessage> conversationHistory) throws JsonProcessingException {
        String history = formatConversationHistory(conversationHistory);
        String prompt = interactiveSearchPrompt
                .replace("{conversation_history}", history)
                .replace("{user_message}", userMessage.getMessage());

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("anthropic_version", "bedrock-2023-05-31");
        payload.put("max_tokens", 4096);
        List<ObjectNode> messages = new ArrayList<>();
        ObjectNode userMessageNode = objectMapper.createObjectNode();
        userMessageNode.put("role", "user");
        userMessageNode.put("content", prompt);
        messages.add(userMessageNode);
        payload.set("messages", objectMapper.valueToTree(messages));

        String payloadJson = objectMapper.writeValueAsString(payload);
        SdkBytes body = SdkBytes.fromUtf8String(payloadJson);

        InvokeModelRequest request = InvokeModelRequest.builder()
                .modelId(bedrockModelId)
                .contentType("application/json")
                .accept("application/json")
                .body(body)
                .build();

        InvokeModelResponse response = bedrockClient.invokeModel(request);
        String responseBodyString = response.body().asUtf8String();

        try {
            JsonNode responseJson = objectMapper.readTree(responseBodyString);
            JsonNode contentBlock = responseJson.path("content");
            if (contentBlock.isArray() && contentBlock.size() > 0) {
                return contentBlock.get(0).path("text").asText("").trim();
            }
        } catch (IOException e) {
            logger.error("Error parsing bedrock response", e);
        }

        return "Sorry, I encountered an error processing the response from the AI model.";
    }

    private JsonNode extractJsonFromResponse(String response) {
        if (response == null) {
            return null;
        }
        // The model may wrap the JSON in markdown ```json ... ```, so we need to extract it.
        int jsonStart = response.indexOf('{');
        int jsonEnd = response.lastIndexOf('}');

        if (jsonStart != -1 && jsonEnd != -1 && jsonEnd > jsonStart) {
            String jsonString = response.substring(jsonStart, jsonEnd + 1);
            try {
                return objectMapper.readTree(jsonString);
            } catch (JsonProcessingException e) {
                logger.warn("Could not parse extracted string as JSON: {}", jsonString);
                return null;
            }
        }
        return null;
    }

    private String formatConversationHistory(List<ChatMessage> messages) {
        StringBuilder sb = new StringBuilder();
        for (ChatMessage message : messages) {
            sb.append(message.getSender().name()).append(": ").append(message.getMessage()).append("\n");
        }
        return sb.toString();
    }

    private List<ContentChunk> performSearch(JsonNode searchParams) throws IOException {
        String query = searchParams.has("query") ? searchParams.get("query").asText() : "";
        String originalFieldName = searchParams.has("original_field_name") ? searchParams.get("original_field_name").asText() : null;
        int limit = searchParams.has("limit") ? searchParams.get("limit").asInt() : 10;
        List<String> tags = searchParams.has("tags") ? objectMapper.convertValue(searchParams.get("tags"), new TypeReference<List<String>>() {}) : null;
        List<String> keywords = searchParams.has("keywords") ? objectMapper.convertValue(searchParams.get("keywords"), new TypeReference<List<String>>() {}) : null;

        // The new search signature expects a Map for context. Since this flow doesn't use it, we pass null.
        return vectorSearchService.search(query, originalFieldName, limit, tags, keywords, null);
    }

    private String formatResults(List<ContentChunk> results) {
        if (results.isEmpty()) {
            return "I couldn't find any results matching your query.";
        }

        StringBuilder sb = new StringBuilder("Here's what I found:\n\n");
        for (ContentChunk chunk : results) {
            sb.append("**Source:** ").append(chunk.getConsolidatedEnrichedSection().getOriginalFieldName()).append("\n");
            sb.append("**Section:** ").append(chunk.getSectionPath()).append("\n");
            sb.append("**Text:** ").append(chunk.getChunkText()).append("\n\n");
        }
        return sb.toString();
    }
}
