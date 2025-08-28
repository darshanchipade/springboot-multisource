package com.apple.springboot.repository;

import com.apple.springboot.model.ContentChunk;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

@Repository
public class ContentChunkRepositoryImpl implements ContentChunkRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public List<ContentChunk> findSimilar(float[] embedding, String originalFieldName,String[] tags, String[] keywords, String[] contextPath, String contextValue, int limit) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT c.* FROM content_chunks c JOIN consolidated_enriched_sections s ON c.consolidated_enriched_section_id = s.id WHERE 1=1");

        Map<String, Object> params = new HashMap<>();

        if (embedding != null) {
            params.put("embedding", embedding);
        }
        if (originalFieldName != null && !originalFieldName.isBlank()) {
            sql.append(" AND LOWER(s.original_field_name) = LOWER(:originalFieldName)");
            params.put("originalFieldName", originalFieldName);
        }

        if (tags != null && tags.length > 0) {
            sql.append(" AND s.tags @> CAST(:tags AS text[])");
            params.put("tags", tags);
        }

        if (keywords != null && keywords.length > 0) {
            sql.append(" AND s.keywords @> CAST(:keywords AS text[])");
            params.put("keywords", keywords);
        }

        if (contextPath != null && contextPath.length > 0 && contextValue != null) {
            sql.append(" AND s.context #>> :contextPath = :contextValue");
            params.put("contextPath", contextPath);
            params.put("contextValue", contextValue);
        }

        if (embedding != null) {
            sql.append(" ORDER BY l2_distance(c.vector, CAST(:embedding AS vector))");
        }

        sql.append(" LIMIT :limit");
        params.put("limit", limit);

        Query query = entityManager.createNativeQuery(sql.toString(), ContentChunk.class);
        params.forEach(query::setParameter);

        return query.getResultList();
    }
}
