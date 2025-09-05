package com.apple.springboot.service;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.repository.ContentChunkRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public class VectorSearchService {

    @Autowired
    private ContentChunkRepository contentChunkRepository;

    @Autowired
    private BedrockEnrichmentService bedrockEnrichmentService;

    public List<ContentChunk> search(String query, String original_field_name, int limit, List<String> tags, List<String> keywords, Map<String, List<String>> context) throws IOException {
        float[] queryVector = bedrockEnrichmentService.generateEmbedding(query);

        String field_name = (original_field_name != null && !original_field_name.isEmpty()) ? original_field_name.toLowerCase() : null;

        return contentChunkRepository.findSimilar(queryVector, field_name, tags, keywords, context, limit);
    }
}
