package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class FacetedSearchService {

    private final VectorSearchService vectorSearchService;
    private final ConsolidatedEnrichedSectionRepository enrichedSectionRepository;
    private final ObjectMapper objectMapper;

    @Autowired
    public FacetedSearchService(VectorSearchService vectorSearchService,
                                ConsolidatedEnrichedSectionRepository enrichedSectionRepository,
                                ObjectMapper objectMapper) {
        this.vectorSearchService = vectorSearchService;
        this.enrichedSectionRepository = enrichedSectionRepository;
        this.objectMapper = objectMapper;
    }

    public FacetResponse getFacets(String query) throws IOException {
        List<ConsolidatedEnrichedSection> relevantSections = enrichedSectionRepository.findByFullTextSearch(query);

        if (relevantSections.isEmpty()) {
            return new FacetResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyMap());
        }

        Set<String> tags = new HashSet<>();
        Set<String> keywords = new HashSet<>();
        Map<String, Set<String>> contextFacets = new HashMap<>();

        for (ConsolidatedEnrichedSection section : relevantSections) {
            if (section.getTags() != null) {
                tags.addAll(section.getTags());
            }
            if (section.getKeywords() != null) {
                keywords.addAll(section.getKeywords());
            }
            extractContextFacets(section.getContext(), contextFacets);
        }

        FacetResponse response = new FacetResponse(
                new ArrayList<>(tags),
                new ArrayList<>(keywords),
                contextFacets.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue())))
        );

        try {
            System.out.println("DEBUG: Final FacetResponse being sent to UI: " + objectMapper.writeValueAsString(response));
        } catch (JsonProcessingException e) {
            System.out.println("DEBUG: Error serializing FacetResponse for logging.");
        }

        return response;
    }

    private void extractContextFacets(Map<String, Object> contextMap, Map<String, Set<String>> contextFacets) {
        if (contextMap == null || contextMap.isEmpty()) {
            return;
        }

        JsonNode contextNode = objectMapper.valueToTree(contextMap);

        // Keys to be extracted from the 'facets' object
        if (contextNode.has("facets")) {
            JsonNode facetsNode = contextNode.get("facets");
            List<String> facetKeys = List.of("sectionKey", "eventType", "sectionModel");
            extractKeysFromNode(facetsNode, facetKeys, contextFacets);
        }

        // Keys to be extracted from the 'envelope' object
        if (contextNode.has("envelope")) {
            JsonNode envelopeNode = contextNode.get("envelope");
            List<String> envelopeKeys = List.of("country", "locale", "pathHierarchy");
            extractKeysFromNode(envelopeNode, envelopeKeys, contextFacets);
        }
    }

    private void extractKeysFromNode(JsonNode parentNode, List<String> keys, Map<String, Set<String>> contextFacets) {
        for (String key : keys) {
            if (parentNode.has(key)) {
                JsonNode valueNode = parentNode.get(key);
                if (valueNode.isTextual()) {
                    contextFacets.computeIfAbsent(key, k -> new HashSet<>()).add(valueNode.asText());
                } else if (valueNode.isArray()) {
                    valueNode.forEach(item -> {
                        if (item.isTextual()) {
                            contextFacets.computeIfAbsent(key, k -> new HashSet<>()).add(item.asText());
                        }
                    });
                }
            }
        }
    }

    public List<SearchResult> search(SearchRequest request) throws IOException {
        List<ContentChunk> searchResults = vectorSearchService.search(
                request.getQuery(),
                null,
                10,
                request.getTags(),
                request.getKeywords(),
                request.getContext()
        );

        return searchResults.stream()
                .map(chunk -> new SearchResult(
                        chunk.getConsolidatedEnrichedSection().getCleansedText(),
                        chunk.getConsolidatedEnrichedSection().getOriginalFieldName(),
                        chunk.getSectionPath(),
                        chunk.getScore() != null ? chunk.getScore() : 0.0f
                ))
                .collect(Collectors.toList());
    }
}
