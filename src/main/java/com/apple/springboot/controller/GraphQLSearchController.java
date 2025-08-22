package com.apple.springboot.controller;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.service.VectorSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;

@Controller
public class GraphQLSearchController {

    private final VectorSearchService vectorSearchService;

    @Autowired
    public GraphQLSearchController(VectorSearchService vectorSearchService) {
        this.vectorSearchService = vectorSearchService;
    }

    @QueryMapping
    public List<ContentChunk> search(@Argument String query, @Argument Double threshold, @Argument Integer limit, @Argument List<String> tags, @Argument List<String> keywords, @Argument String contextKey, @Argument String contextValue) {
        double thresholdVal = (threshold != null) ? threshold : 0.8;
        int limitVal = (limit != null) ? limit : 10;
        return vectorSearchService.search(query, thresholdVal, limitVal, tags, keywords, contextKey, contextValue);
    }
}
