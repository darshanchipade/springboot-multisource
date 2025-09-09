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
        List<ContentChunkWithDistance> initialChunks = vectorSearchService.search(query, null, 20, null, null, null);

        if (initialChunks.isEmpty()) {
            return Collections.emptyList();
        }

        Map<RefinementChip, Double> chipScores = new HashMap<>();

        for (ContentChunkWithDistance chunkWithDistance : initialChunks) {
            double distance = chunkWithDistance.getDistance();
            double score = 1.0 - distance; // Smaller distance = higher score

            if (score < 0) continue; // Ignore documents that are too dissimilar

            ConsolidatedEnrichedSection section = chunkWithDistance.getContentChunk().getConsolidatedEnrichedSection();
            if (section == null) continue;

            // Extract from simple fields
            if (section.getTags() != null) {
                section.getTags().forEach(tag -> {
                    RefinementChip chip = new RefinementChip(tag, "Tag", 0);
                    chipScores.merge(chip, score, Double::sum);
                });
            }
            if (section.getKeywords() != null) {
                section.getKeywords().forEach(keyword -> {
                    RefinementChip chip = new RefinementChip(keyword, "Keyword", 0);
                    chipScores.merge(chip, score, Double::sum);
                });
            }
            if (section.getOriginalFieldName() != null && !section.getOriginalFieldName().isBlank()) {
                RefinementChip chip = new RefinementChip(section.getOriginalFieldName(), "Original Field", 0);
                chipScores.merge(chip, score, Double::sum);
            }

            // Extract from nested context
            if (section.getContext() != null) {
                JsonNode contextNode = objectMapper.valueToTree(section.getContext());
                extractContextChips(contextNode.path("facets"), List.of("sectionKey", "eventType", "sectionModel"), chipScores, score);
                extractContextChips(contextNode.path("envelope"), List.of("country", "locale"), chipScores, score);

                // Special handling for pathHierarchy
                JsonNode pathHierarchyNode = contextNode.path("envelope").path("pathHierarchy");
                if (pathHierarchyNode.isArray()) {
                    pathHierarchyNode.forEach(path -> {
                        if (path.isTextual() && !path.asText().isBlank()) {
                            RefinementChip chip = new RefinementChip(path.asText(), "pathHierarchy", 0);
                            chipScores.merge(chip, score, Double::sum);
                        }
                    });
                }
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

    private void extractContextChips(JsonNode parentNode, List<String> keys, Map<RefinementChip, Double> chipScores, double score) {
        if (parentNode.isMissingNode()) return;

        for (String key : keys) {
            JsonNode valueNode = parentNode.path(key);
            if (valueNode.isTextual() && !valueNode.asText().isBlank()) {
                RefinementChip chip = new RefinementChip(valueNode.asText(), "Context:" + key, 0);
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
        if (section.getOriginalFieldName() != null && !section.getOriginalFieldName().isBlank()) {
            chips.add(new RefinementChip(section.getOriginalFieldName(), "Original Field", 0));
        }
        if (section.getContext() != null) {
            JsonNode contextNode = objectMapper.valueToTree(section.getContext());
            extractContextChipsForCounting(contextNode.path("facets"), List.of("sectionKey", "eventType", "sectionModel"), chips);
            extractContextChipsForCounting(contextNode.path("envelope"), List.of("country", "locale"), chips);
            JsonNode pathHierarchyNode = contextNode.path("envelope").path("pathHierarchy");
            if (pathHierarchyNode.isArray()) {
                pathHierarchyNode.forEach(path -> {
                    if (path.isTextual() && !path.asText().isBlank()) {
                        chips.add(new RefinementChip(path.asText(), "pathHierarchy", 0));
                    }
                });
            }
        }
        return chips;
    }

    private void extractContextChipsForCounting(JsonNode parentNode, List<String> keys, List<RefinementChip> chips) {
        if (parentNode.isMissingNode()) return;

        for (String key : keys) {
            JsonNode valueNode = parentNode.path(key);
            if (valueNode.isTextual() && !valueNode.asText().isBlank()) {
                chips.add(new RefinementChip(valueNode.asText(), "Context:" + key, 0));
            }
        }
    }
}
