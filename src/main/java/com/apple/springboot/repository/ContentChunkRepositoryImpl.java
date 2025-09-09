package com.apple.springboot.repository;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.persistence.Tuple;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

@Repository
public class ContentChunkRepositoryImpl implements ContentChunkRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<ContentChunkWithDistance> findSimilar(float[] embedding, String originalFieldName, String[] tags, String[] keywords, Map<String, Object> contextMap, int limit) {
        StringBuilder sql = new StringBuilder("SELECT c.*");
        if (embedding != null) {
            sql.append(", (c.vector <=> CAST(:embedding AS vector)) as distance");
        }
        sql.append(" FROM content_chunks c JOIN consolidated_enriched_sections s ON c.consolidated_enriched_section_id = s.id WHERE 1=1");

        Map<String, Object> params = new HashMap<>();

        if (embedding != null) {
            params.put("embedding", embedding);
        }
        if (originalFieldName != null && !originalFieldName.isBlank()) {
            sql.append(" AND LOWER(s.original_field_name) = LOWER(:originalFieldName)");
            params.put("originalFieldName", originalFieldName);
        }

        if (tags != null && tags.length > 0) {
            for (int i = 0; i < tags.length; i++) {
                String paramName = "tag" + i;
                sql.append(" AND EXISTS (SELECT 1 FROM unnest(s.tags) db_tag WHERE db_tag LIKE '%' || :").append(paramName).append(" || '%')");
                params.put(paramName, tags[i]);
            }
        }

        if (keywords != null && keywords.length > 0) {
            for (int i = 0; i < keywords.length; i++) {
                String paramName = "keywords" + i;
                sql.append(" AND EXISTS (SELECT 1 FROM unnest(s.keywords) db_keywords WHERE db_keywords LIKE '%' || :").append(paramName).append(" || '%')");
                params.put(paramName, keywords[i]);
            }
        }

        if (contextMap != null && !contextMap.isEmpty()) {
            try {
                String jsonbFilter = objectMapper.writeValueAsString(contextMap);
                sql.append(" AND s.context @> :jsonbFilter::jsonb");
                params.put("jsonbFilter", jsonbFilter);
            } catch (JsonProcessingException e) {
                // Handle exception, maybe log it
            }
        }

        if (embedding != null) {
            sql.append(" ORDER BY distance");
        }

        sql.append(" LIMIT :limit");
        params.put("limit", limit);

        Query query = entityManager.createNativeQuery(sql.toString(), "ContentChunkWithDistanceMapping");
        params.forEach(query::setParameter);

        List<Object[]> results = query.getResultList();
        List<ContentChunkWithDistance> dtos = new ArrayList<>();
        for (Object[] result : results) {
            ContentChunk chunk = (ContentChunk) result[0];
            double distance = (embedding != null) ? ((Number) result[1]).doubleValue() : 0.0;
            dtos.add(new ContentChunkWithDistance(chunk, distance));
        }
        return dtos;
    }
}
