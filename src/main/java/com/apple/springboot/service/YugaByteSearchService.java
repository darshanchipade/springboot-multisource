package com.apple.springboot.service;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.repository.ContentChunkRepository;
import com.yugabyte.ysql.type.Vector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.util.List;

@Service
public class YugaByteSearchService {

    @Autowired
    private ContentChunkRepository contentChunkRepository;

    @PersistenceContext
    private EntityManager entityManager;

    public List<ContentChunk> search(String query, double threshold, int limit) {
        // 1. Generate embedding for the query using YugabyteDB's built-in function
        String sql = "SELECT text_to_embedding('sentence-transformers/all-MiniLM-L6-v2', :query)";
        Vector queryVector = (Vector) entityManager.createNativeQuery(sql, Vector.class)
                .setParameter("query", query)
                .getSingleResult();

        // 2. Search for the nearest neighbors
        return contentChunkRepository.findNearestNeighborsNative(queryVector, threshold, limit);
    }
}
