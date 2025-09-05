package com.apple.springboot.repository;

import com.apple.springboot.model.ContentChunk;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

@Repository
public class ContentChunkRepositoryImpl implements ContentChunkRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<ContentChunk> findSimilar(float[] embedding, String originalFieldName, List<String> tags, List<String> keywords, Map<String, List<String>> context, int limit) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT c.*, l2_distance(c.vector, CAST(:embedding AS vector)) AS score FROM content_chunks c JOIN consolidated_enriched_section s ON c.consolidated_enriched_section_id = s.id WHERE 1=1");

        Map<String, Object> params = new HashMap<>();

        if (embedding != null) {
            params.put("embedding", embedding);
        }
        if (originalFieldName != null && !originalFieldName.isBlank()) {
            sql.append(" AND LOWER(s.original_field_name) = LOWER(:originalFieldName)");
            params.put("originalFieldName", originalFieldName);
        }

        if (tags != null && !tags.isEmpty()) {
            sql.append(" AND s.tags @> CAST(string_to_array(:tags, ',') AS text[])");
            params.put("tags", String.join(",", tags));
        }

        if (keywords != null && !keywords.isEmpty()) {
            sql.append(" AND s.keywords @> CAST(string_to_array(:keywords, ',') AS text[])");
            params.put("keywords", String.join(",", keywords));
        }

        if (context != null && !context.isEmpty()) {
            // Separate pathHierarchy from other context filters because it requires a different query structure
            Map<String, List<String>> otherContextFilters = new HashMap<>(context);
            List<String> pathHierarchyValues = otherContextFilters.remove("pathHierarchy");

            // Handle all other context filters with a single JSON object
            if (!otherContextFilters.isEmpty()) {
                Map<String, Object> queryJson = new HashMap<>();
                Map<String, String> facetsMap = new HashMap<>();
                Map<String, String> envelopeMap = new HashMap<>();
                List<String> facetKeys = List.of("sectionKey", "eventType", "sectionModel");

                for (Map.Entry<String, List<String>> entry : otherContextFilters.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue().get(0); // Assuming single value
                    if (facetKeys.contains(key)) {
                        facetsMap.put(key, value);
                    } else {
                        envelopeMap.put(key, value);
                    }
                }
                if (!facetsMap.isEmpty()) {
                    queryJson.put("facets", facetsMap);
                }
                if (!envelopeMap.isEmpty()) {
                    queryJson.put("envelope", envelopeMap);
                }

                try {
                    String contextJson = objectMapper.writeValueAsString(queryJson);
                    sql.append(" AND s.context @> CAST(:contextJson AS jsonb)");
                    params.put("contextJson", contextJson);
                } catch (JsonProcessingException e) {
                    // Handle exception
                }
            }

            // Handle pathHierarchy with its own specific array containment query
            if (pathHierarchyValues != null && !pathHierarchyValues.isEmpty()) {
                String pathValue = pathHierarchyValues.get(0);
                try {
                    // Let Jackson correctly serialize the string into a JSON string literal (e.g., "education")
                    // for the array containment check.
                    String jsonStringValue = objectMapper.writeValueAsString(pathValue);
                    sql.append(" AND s.context -> 'envelope' -> 'pathHierarchy' @> CAST(:pathValue AS jsonb)");
                    params.put("pathValue", jsonStringValue);
                } catch (JsonProcessingException e) {
                    // Handle exception
                }
            }
        }

        if (embedding != null) {
            sql.append(" ORDER BY score");
        }

        sql.append(" LIMIT :limit");
        params.put("limit", limit);

        Query query = entityManager.createNativeQuery(sql.toString(), "ContentChunkWithScore");
        params.forEach(query::setParameter);

        List<Object[]> results = query.getResultList();
        return results.stream()
                .map(record -> {
                    ContentChunk chunk = (ContentChunk) record[0];
                    Float score = (Float) record[1];
                    chunk.setScore(score);
                    return chunk;
                })
                .collect(Collectors.toList());
    }
}
