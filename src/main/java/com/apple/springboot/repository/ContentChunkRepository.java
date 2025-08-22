package com.apple.springboot.repository;

import com.apple.springboot.model.ContentChunk;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ContentChunkRepository extends JpaRepository<ContentChunk, UUID> {

    // @Query(value = "SELECT * FROM content_chunks ORDER BY vector <=> CAST(:queryVector AS vector) LIMIT :limit", nativeQuery = true)
    //  List<ContentChunk> findNearestNeighbors(@Param("queryVector") float[] queryVector, @Param("limit") int limit);
    //  List<ContentChunk> findNearestNeighbors(@Param("queryVector") String vectorStr, @Param("limit") int limit);

    //@Query(value = "SELECT * FROM content_chunks WHERE vector <=> CAST(:queryVector AS vector(1024)) < :distanceThreshold ORDER BY vector <=> CAST(:queryVector AS vector(1024))  LIMIT :limit", nativeQuery = true)

    @Query(value = "SELECT cc.* FROM content_chunks cc JOIN consolidated_enriched_section ces ON cc.consolidated_enriched_section_id = ces.id WHERE (COALESCE(:tags) IS NULL OR ces.tags @> CAST(:tags AS text[])) AND (COALESCE(:keywords) IS NULL OR ces.keywords @> CAST(:keywords AS text[])) AND (:contextKey IS NULL OR (ces.context ->> :contextKey) = :contextValue) AND cc.vector <-> CAST(:queryVector AS vector) < :distanceThreshold ORDER BY cc.vector <-> CAST(:queryVector AS vector) LIMIT :limit", nativeQuery = true)
    List<ContentChunk> findNearestNeighbors(
            @Param("queryVector") float[] queryVector,
            @Param("distanceThreshold") double threshold,
            @Param("limit") int limit,
            @Param("tags") List<String> tags,
            @Param("keywords") List<String> keywords,
            @Param("contextKey") String contextKey,
            @Param("contextValue") String contextValue
    );
}

