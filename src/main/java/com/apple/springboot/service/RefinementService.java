package com.apple.springboot.service;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.RefinementChip;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class RefinementService {

    @Autowired
    private VectorSearchService vectorSearchService;

    @Autowired
    private ObjectMapper objectMapper;

    public List<RefinementChip> getRefinementChips(String query) throws IOException {
        // Perform a pure semantic search with a balanced threshold to get relevant documents.
        Double threshold = 0.9;
        List<ContentChunkWithDistance> initialChunks = vectorSearchService.search(query, null, 20, null, null, null, threshold);

        if (initialChunks.isEmpty()) {
            return Collections.emptyList();
        }

        Map<RefinementChip, Double> chipScores = new HashMap<>();

        for (ContentChunkWithDistance chunkWithDistance : initialChunks) {
            double distance = chunkWithDistance.getDistance();
            double score = 1.0 - distance;

            if (score < 0) continue;

            ConsolidatedEnrichedSection section = chunkWithDistance.getContentChunk().getConsolidatedEnrichedSection();
            if (section == null) continue;

            // Extract Tags
            if (section.getTags() != null) {
                section.getTags().forEach(tag -> {
                    RefinementChip chip = new RefinementChip(tag, "Tag", 0);
                    chipScores.merge(chip, score, Double::sum);
                });
            }
            // Extract Keywords
            if (section.getKeywords() != null) {
                section.getKeywords().forEach(keyword -> {
                    RefinementChip chip = new RefinementChip(keyword, "Keyword", 0);
                    chipScores.merge(chip, score, Double::sum);
                });
            }

            // Extract from nested context based on simplified requirements
            if (section.getContext() != null) {
                JsonNode contextNode = objectMapper.valueToTree(section.getContext());
                extractContextChips(contextNode.path("facets"), List.of("sectionModel", "eventType"), "facets", chipScores, score);
                extractContextChips(contextNode.path("envelope"), List.of("sectionName", "locale", "country"), "envelope", chipScores, score);
            }
        }

        // Get the count for each chip for display
        Map<RefinementChip, Long> chipCounts = initialChunks.stream()
                .map(chunk -> chunk.getContentChunk().getConsolidatedEnrichedSection())
                .filter(Objects::nonNull)
                .flatMap(section -> extractChipsForCounting(section).stream())
                .collect(Collectors.groupingBy(chip -> chip, Collectors.counting()));


        return chipScores.entrySet().stream()
                .sorted(Map.Entry.<RefinementChip, Double>comparingByValue().reversed())
                .limit(10)
                .map(entry -> {
                    RefinementChip chip = entry.getKey();
                    chip.setCount(chipCounts.getOrDefault(chip, 0L).intValue());
                    return chip;
                })
                .collect(Collectors.toList());
    }

    private void extractContextChips(JsonNode parentNode, List<String> keys, String pathPrefix, Map<RefinementChip, Double> chipScores, double score) {
        if (parentNode.isMissingNode()) return;

        for (String key : keys) {
            JsonNode valueNode = parentNode.path(key);
            if (valueNode.isTextual() && !valueNode.asText().isBlank()) {
                RefinementChip chip = new RefinementChip(valueNode.asText(), "Context:" + pathPrefix + "." + key, 0);
                chipScores.merge(chip, score, Double::sum);
            }
        }
    }

    private List<RefinementChip> extractChipsForCounting(ConsolidatedEnrichedSection section) {
        List<RefinementChip> chips = new ArrayList<>();
        if (section.getTags() != null) {
            section.getTags().forEach(tag -> chips.add(new RefinementChip(tag, "Tag", 0)));
        }
        if (section.getKeywords() != null) {
            section.getKeywords().forEach(keyword -> chips.add(new RefinementChip(keyword, "Keyword", 0)));
        }
        if (section.getContext() != null) {
            JsonNode contextNode = objectMapper.valueToTree(section.getContext());
            extractContextChipsForCounting(contextNode.path("facets"), List.of("sectionModel", "eventType"), "facets", chips);
            extractContextChipsForCounting(contextNode.path("envelope"), List.of("sectionName", "locale", "country"), "envelope", chips);
        }
        return chips;
    }

    private void extractContextChipsForCounting(JsonNode parentNode, List<String> keys, String pathPrefix, List<RefinementChip> chips) {
        if (parentNode.isMissingNode()) return;

        for (String key : keys) {
            JsonNode valueNode = parentNode.path(key);
            if (valueNode.isTextual() && !valueNode.asText().isBlank()) {
                chips.add(new RefinementChip(valueNode.asText(), "Context:" + pathPrefix + "." + key, 0));
            }
        }
    }
}