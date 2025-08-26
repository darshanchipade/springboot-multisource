package com.apple.springboot.service;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.repository.ContentChunkRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class VectorSearchService {

    @Autowired
    private ContentChunkRepository contentChunkRepository;

    @Autowired
    private BedrockEnrichmentService bedrockEnrichmentService;

    public List<ContentChunk> search(String query, int limit, List<String> tags, List<String> keywords, String[] contextPath, String contextValue) throws IOException {
        float[] queryVector = bedrockEnrichmentService.generateEmbedding(query);

        String[] tagsArray = (tags != null && !tags.isEmpty()) ? tags.toArray(new String[0]) : null;
        String[] keywordsArray = (keywords != null && !keywords.isEmpty()) ? keywords.toArray(new String[0]) : null;
        String[] contextPathArray = (contextPath != null && contextPath.length > 0) ? contextPath : null;

        return contentChunkRepository.findSimilar(queryVector, tagsArray, keywordsArray, contextPathArray, contextValue, limit);
    }
}
