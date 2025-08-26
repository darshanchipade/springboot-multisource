package com.apple.springboot.repository;

import com.apple.springboot.model.ContentChunk;

import java.util.List;

public interface ContentChunkRepositoryCustom {
    List<ContentChunk> findSimilar(
            float[] embedding,
            String[] tags,
            String[] keywords,
            String[] contextPath,
            String contextValue,
            //double threshold,
            int limit
    );
}
