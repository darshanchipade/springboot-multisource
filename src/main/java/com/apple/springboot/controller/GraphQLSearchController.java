package com.apple.springboot.controller;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.model.FacetResponse;
import com.apple.springboot.service.FacetedSearchService;
import com.apple.springboot.service.VectorSearchService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Controller
public class GraphQLSearchController {

    private final VectorSearchService vectorSearchService;
    private final FacetedSearchService facetedSearchService;
    private final ObjectMapper objectMapper;

    @Autowired
    public GraphQLSearchController(VectorSearchService vectorSearchService, FacetedSearchService facetedSearchService, ObjectMapper objectMapper) {
        this.vectorSearchService = vectorSearchService;
        this.facetedSearchService = facetedSearchService;
        this.objectMapper = objectMapper;
    }

    @QueryMapping
    public List<ContentChunk> search(@Argument String query, @Argument String original_field_name, @Argument Integer limit, @Argument List<String> tags, @Argument List<String> keywords, @Argument String context) throws IOException {
        int limitVal = (limit != null) ? limit : 10;
        Map<String, List<String>> contextMap = null;
        if (context != null && !context.isEmpty()) {
            contextMap = objectMapper.readValue(context, new TypeReference<Map<String, List<String>>>() {});
        }
        return vectorSearchService.search(query, original_field_name, limitVal, tags, keywords, contextMap);
    }

    @QueryMapping
    public FacetResponse getFacets(@Argument String query) throws IOException {
        return facetedSearchService.getFacets(query);
    }
}
