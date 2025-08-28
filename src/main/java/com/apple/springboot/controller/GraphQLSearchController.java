package com.apple.springboot.controller;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.service.VectorSearchService;
import java.io.IOException;
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
    public List<ContentChunk> search(@Argument String query, @Argument String original_field_name, @Argument Integer limit, @Argument List<String> tags, @Argument List<String> keywords, @Argument List<String> contextPath, @Argument String contextValue) throws IOException {
        int limitVal = (limit != null) ? limit : 10;
        String[] contextPathArray = (contextPath != null) ? contextPath.toArray(new String[0]) : null;
        return vectorSearchService.search(query, original_field_name, limitVal, tags, keywords, contextPathArray, contextValue);
    }
}
