package com.apple.springboot.repository;

import com.apple.springboot.model.ContentChunk;

import java.util.List;
import java.util.Map;

public interface ContentChunkRepositoryCustom {
    List<ContentChunk> findSimilar(
            float[] embedding,
            String originalFieldName,
            List<String> tags,
            List<String> keywords,
            Map<String, List<String>> context,
            int limit
    );
}
