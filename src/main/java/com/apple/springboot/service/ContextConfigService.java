package com.apple.springboot.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ContextConfigService {

    private static final Logger logger = LoggerFactory.getLogger(ContextConfigService.class);

    private final ResourceLoader resourceLoader;
    private final ObjectMapper objectMapper;
    private final String contextConfigPath;

    private Map<String, Object> defaultContext = Collections.emptyMap();
    private List<ContextRule> contextRules = new ArrayList<>();

    // Inner class to represent a rule from context-config.json
    private static class ContextRule {
        public int priority = Integer.MAX_VALUE;
        public String ruleName = "Unnamed Rule";
        public Map<String, String> matchCriteria;
        public Map<String, Object> context;

        public ContextRule() {}

        @Override
        public String toString() {
            return "ContextRule{" +
                    "priority=" + priority +
                    ", ruleName='" + ruleName + '\'' +
                    ", matchCriteria=" + matchCriteria +
                    ", context=" + context +
                    '}';
        }
    }

    public ContextConfigService(ResourceLoader resourceLoader,
                                ObjectMapper objectMapper,
                                @Value("${app.context.config.path:classpath:context-config.json}") String contextConfigPath) {
        this.resourceLoader = resourceLoader;
        this.objectMapper = objectMapper;
        this.contextConfigPath = contextConfigPath;
    }

    @PostConstruct
    public void loadContextConfiguration() {
        Resource resource = resourceLoader.getResource(contextConfigPath);
        if (!resource.exists()) {
            logger.error("Context configuration file not found at: {}. Using empty default and no rules.", contextConfigPath);
            defaultContext = Map.of("error", "Default context config file not loaded: " + contextConfigPath);
            contextRules = Collections.emptyList();
            return;
        }

        try (InputStream inputStream = resource.getInputStream()) {
            Map<String, JsonNode> fullConfig = objectMapper.readValue(inputStream, new TypeReference<Map<String, JsonNode>>() {});

            JsonNode defaultContextNode = fullConfig.get("defaultContext");
            if (defaultContextNode != null && defaultContextNode.isObject()) {
                defaultContext = objectMapper.convertValue(defaultContextNode, new TypeReference<Map<String, Object>>() {});
                logger.info("Default context loaded successfully: {}", defaultContext);
            } else {
                logger.warn("No 'defaultContext' object found or it's not an object in {}. Using empty default.", contextConfigPath);
                defaultContext = Map.of("warning", "Default context not defined or invalid in config");
            }

            JsonNode rulesNode = fullConfig.get("contextRules");
            if (rulesNode != null && rulesNode.isArray()) {
                contextRules = objectMapper.convertValue(rulesNode, new TypeReference<List<ContextRule>>() {});
                contextRules.sort(Comparator.comparingInt(rule -> rule.priority));
                logger.info("Loaded and sorted {} context rules from {}", contextRules.size(), contextConfigPath);
                if (logger.isDebugEnabled()) {
                    contextRules.forEach(rule -> logger.debug("Loaded rule: {}", rule));
                }
            } else {
                logger.info("No 'contextRules' array found or it's not an array in {}. No specific rules loaded.", contextConfigPath);
                contextRules = Collections.emptyList();
            }

        } catch (IOException e) {
            logger.error("Failed to load or parse context configuration from {}: {}. Using empty default and no rules.", contextConfigPath, e.getMessage(), e);
            defaultContext = Map.of("error", "IOException during context config load: " + e.getMessage());
            contextRules = Collections.emptyList();
        } catch (Exception e) {
            logger.error("Error processing context configuration structure in {}: {}. Using empty default and no rules.", contextConfigPath, e.getMessage(), e);
            defaultContext = Map.of("error", "Invalid structure or data type in context config: " + e.getMessage());
            contextRules = Collections.emptyList();
        }
    }

    public Map<String, Object> getDefaultContext() {
        return Collections.unmodifiableMap(new HashMap<>(defaultContext));
    }

    public Map<String, Object> getContext(String model, String path) {
        if (path == null) path = "";
        if (model == null) model = "";

        Map<String, Object> mergedContext = new HashMap<>(defaultContext);
        List<String> matchedRuleNames = new ArrayList<>();

        for (ContextRule rule : contextRules) {
            if (matches(rule.matchCriteria, model, path)) {
                logger.debug("Rule '{}' (priority {}) matched for model '{}', path '{}'. Merging context.",
                        rule.ruleName, rule.priority, model, path);
                matchedRuleNames.add(rule.ruleName + " (priority " + rule.priority + ")");
                mergeContextsAggressively(mergedContext, rule.context);
            }
        }

        if (matchedRuleNames.isEmpty()) {
            logger.debug("No specific rules matched for model '{}', path '{}'. Returning default context.", model, path);
        } else {
            // mergedContext.put("appliedRuleNames", matchedRuleNames); // Optional for debugging
            logger.info("Final merged context for model '{}', path '{}' from rules: {}. Context: {}",
                    model, path, matchedRuleNames, mergedContext);
        }
        return Collections.unmodifiableMap(mergedContext);
    }

    @SuppressWarnings("unchecked")
    private void mergeContextsAggressively(Map<String, Object> baseContext, Map<String, Object> newRuleContext) {
        if (newRuleContext == null) return;

        // newRuleContext.forEach((key, valueFromNewRule) -> {
        for (Map.Entry<String, Object> entry : newRuleContext.entrySet()) {
            String key = entry.getKey();
            Object valueFromNewRule = entry.getValue();
            if (baseContext.containsKey(key)) {
                Object valueInBase = baseContext.get(key);
                List<Object> combinedList;

                if (valueInBase instanceof List) {
                    combinedList = new ArrayList<>((List<Object>) valueInBase);
                } else {
                    combinedList = new ArrayList<>();
                    combinedList.add(valueInBase);
                }

                if (valueFromNewRule instanceof List) {
                    ((List<Object>) valueFromNewRule).forEach(item -> {
                        if (!combinedList.contains(item)) { // Add if not already present
                            combinedList.add(item);
                        }
                    });
                } else {
                    if (!combinedList.contains(valueFromNewRule)) {
                        combinedList.add(valueFromNewRule);
                    }
                }
                // If after merging, the list has only one item, and it's not one of our intentionally multi-valued keys,
                // consider reverting to a single value. For now, let's keep them as lists if they become lists.
                baseContext.put(key, combinedList);
            } else {
                // New key, just add it. If it's a list from the rule, it's added as a list.
                baseContext.put(key, valueFromNewRule);
            }
        };
    }

    private boolean matches(Map<String, String> criteria, String model, String path) {
        if (criteria == null || criteria.isEmpty()) return false;

        boolean modelCriteriaMet = !criteria.containsKey("modelIs");
        if (criteria.containsKey("modelIs")) {
            modelCriteriaMet = model.equals(criteria.get("modelIs"));
        }

        boolean pathCriteriaMet = !criteria.containsKey("pathContains");
        if (criteria.containsKey("pathContains")) {
            pathCriteriaMet = path.contains(criteria.get("pathContains"));
        }

        return modelCriteriaMet && pathCriteriaMet;
    }
}
