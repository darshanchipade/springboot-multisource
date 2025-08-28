package com.apple.springboot.service;

import com.apple.springboot.model.EnrichmentContext;

public class CleansedItemDetail {
    public final String sourcePath;
    public final String originalFieldName;
    public final String cleansedContent;
    public final String model;
    public final EnrichmentContext context;

    public CleansedItemDetail(String sourcePath, String originalFieldName, String cleansedContent, String model, EnrichmentContext context) {
        this.sourcePath = sourcePath;
        this.originalFieldName = originalFieldName;
        this.cleansedContent = cleansedContent;
        this.model = model;
        this.context = context;
    }
}
