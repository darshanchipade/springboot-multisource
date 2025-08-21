package com.apple.springboot.service;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.repository.ContentChunkRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
@Service
public class SearchService {

    @Autowired
    private BedrockEnrichmentService bedrockEnrichmentService;

    @Autowired
    private ContentChunkRepository contentChunkRepository;

    public String search(String query) throws IOException {
        float[] embedding = bedrockEnrichmentService.generateEmbedding(query);
        String vectorStr = convertToVectorString(embedding);
        double threshold = 1.2;
        int limit = 5;

        List<ContentChunk> nearestChunks = contentChunkRepository.findNearestNeighbors(vectorStr, threshold, limit);

        if (nearestChunks.isEmpty()) {
            return "No matches found for query: \"" + query + "\"";
        }

        return nearestChunks.stream()
                .map(chunk -> "Match:\n" + chunk.getChunkText())
                .collect(Collectors.joining("\n\n"));
    }
    public String convertToVectorString(float[] vector) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < vector.length; i++) {
            sb.append(String.format("%.6f", vector[i]));
            if (i < vector.length - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
