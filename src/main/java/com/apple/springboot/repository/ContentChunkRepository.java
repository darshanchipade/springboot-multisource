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

    //List<ContentChunk> findNearestNeighbors(@Param("queryVector") String vectorStr, @Param("distanceThreshold") double threshold, @Param("limit") int limit);
//    @Query(value = "SELECT * FROM content_chunks WHERE vector <=> CAST(:queryVector AS vector(1024)) < :distanceThreshold ORDER BY vector <=> CAST(:queryVector AS vector(1024)) LIMIT :limit", nativeQuery = true)
//    List<ContentChunk> findNearestNeighbors(
//            @Param("queryVector") String vectorStr,
//            @Param("distanceThreshold") double threshold,
//            @Param("limit") int limit
//    );
    @Query(value = "SELECT cc.* FROM content_chunks cc " +
            "JOIN consolidated_enriched_section ces ON cc.consolidated_enriched_section_id = ces.id " +
            "WHERE cc.embedding <-> :queryVector < :distanceThreshold " +
            "ORDER BY cc.embedding <-> :queryVector " +
            "LIMIT :limit", nativeQuery = true)
    List<ContentChunk> findNearestNeighborsNative(
            @Param("queryVector") Vector queryVector,
            @Param("distanceThreshold") double threshold,
            @Param("limit") int limit
    );
}

