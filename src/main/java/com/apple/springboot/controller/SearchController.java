
package com.apple.springboot.controller;

import com.apple.springboot.model.FacetResponse;
import com.apple.springboot.model.SearchRequest;
import com.apple.springboot.model.SearchResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

import com.apple.springboot.model.RefinementChip;
import com.apple.springboot.service.RefinementService;

@RestController
@RequestMapping("/api")
public class SearchController {

//    private final FacetedSearchService facetedSearchService;
    private final RefinementService refinementService;

   @Autowired
    public SearchController(RefinementService refinementService) {
        //this.facetedSearchService = facetedSearchService;
        this.refinementService = refinementService;
    }
//
//    @GetMapping("/facets")
//    public FacetResponse getFacets(@RequestParam String query) throws IOException {
//        return facetedSearchService.getFacets(query);
//    }
//
//    @PostMapping("/search")
//    public List<SearchResult> search(@RequestBody SearchRequest request) throws IOException {
//        return facetedSearchService.search(request);
//    }

    @GetMapping("/refine")
    public List<RefinementChip> getRefinementChips(@RequestParam String query) throws IOException {
        return refinementService.getRefinementChips(query);
    }
}
