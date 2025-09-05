package com.apple.springboot.controller;

import com.apple.springboot.model.FacetResponse;
import com.apple.springboot.model.SearchRequest;
import com.apple.springboot.model.SearchResult;
import com.apple.springboot.service.FacetedSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api")
public class SearchController {

    private final FacetedSearchService facetedSearchService;

    @Autowired
    public SearchController(FacetedSearchService facetedSearchService) {
        this.facetedSearchService = facetedSearchService;
    }

    @GetMapping("/facets")
    public FacetResponse getFacets(@RequestParam String query) throws IOException {
        return facetedSearchService.getFacets(query);
    }

    @PostMapping("/search")
    public List<SearchResult> search(@RequestBody SearchRequest request) throws IOException {
        return facetedSearchService.search(request);
    }
}
