package com.apple.springboot.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class AIResponseValidator {

    private static final Logger logger = LoggerFactory.getLogger(AIResponseValidator.class);

    /**
     * Validates the structure of the enriched data map returned by the Bedrock service.
     *
     * @param bedrockResponse The map containing the enrichment results.
     * @return true if the structure is valid, false otherwise.
     */
    public boolean isValid(Map<String, Object> bedrockResponse) {
        if (bedrockResponse == null || bedrockResponse.isEmpty()) {
            logger.warn("Validation failed: Bedrock response map is null or empty.");
            return false;
        }

        if (bedrockResponse.containsKey("error")) {
            logger.warn("Validation failed: Bedrock response contains an explicit error key: {}", bedrockResponse.get("error"));
            return false;
        }

        // Check for standardEnrichments
        Object standardEnrichmentsObj = bedrockResponse.get("standardEnrichments");
        if (standardEnrichmentsObj == null || !(standardEnrichmentsObj instanceof Map)) {
            logger.warn("Validation failed: Missing or invalid 'standardEnrichments' object in response: {}", bedrockResponse);
            return false;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> standardEnrichments = (Map<String, Object>) standardEnrichmentsObj;

        // Validate required fields in standardEnrichments
        if (!isFieldPresentAndOfType(standardEnrichments, "summary", String.class)) {
            logger.warn("Validation failed: 'summary' is missing or not a String in standardEnrichments: {}", standardEnrichments);
            return false;
        }
        if (!isFieldPresentAndOfType(standardEnrichments, "keywords", List.class)) {
            logger.warn("Validation failed: 'keywords' is missing or not a List in standardEnrichments: {}", standardEnrichments);
            return false;
        }
        if (!isFieldPresentAndOfType(standardEnrichments, "sentiment", String.class)) {
            logger.warn("Validation failed: 'sentiment' is missing or not a String in standardEnrichments: {}", standardEnrichments);
            return false;
        }
        if (!isFieldPresentAndOfType(standardEnrichments, "classification", String.class)) {
            logger.warn("Validation failed: 'classification' is missing or not a String in standardEnrichments: {}", standardEnrichments);
            return false;
        }
        if (!isFieldPresentAndOfType(standardEnrichments, "tags", List.class)) {
            logger.warn("Validation failed: 'tags' is missing or not a List in standardEnrichments: {}", standardEnrichments);
            return false;
        }

        logger.debug("AI response validation successful for item.");
        return true;
    }

    private boolean isFieldPresentAndOfType(Map<String, Object> map, String key, Class<?> type) {
        Object value = map.get(key);
        if (value == null) {
            return false;
        }
        return type.isInstance(value);
    }
}