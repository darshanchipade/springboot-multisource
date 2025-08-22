package com.apple.springboot.service;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.repository.ContentChunkRepository;
import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.repository.ContentChunkRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.util.List;

@Service
public class VectorSearchService {

    @Autowired
    private ContentChunkRepository contentChunkRepository;

    @PersistenceContext
    private EntityManager entityManager;

    @Value("${app.embedding.model}")
    private String embeddingModel;

    public List<ContentChunk> search(String query, double threshold, int limit, List<String> tags, List<String> keywords, String contextKey, String contextValue) {
        // 1. Generate embedding for the query using YugabyteDB's built-in function
        String sql = "SELECT text_to_embedding(:model, :query)";
        float[] queryVector = (float[]) entityManager.createNativeQuery(sql)
                .setParameter("model", embeddingModel)
                .setParameter("query", query)
                .getSingleResult();

        // 2. Search for the nearest neighbors
        return contentChunkRepository.findNearestNeighbors(queryVector, threshold, limit, tags, keywords, contextKey, contextValue);
    }
}
