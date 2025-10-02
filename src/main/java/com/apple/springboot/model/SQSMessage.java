package com.apple.springboot.model;

import java.util.UUID;

public class SQSMessage {

    private String jobId;
    private UUID cleansedDataStoreId;
    private String sourcePath;
    private String originalFieldName;
    private String cleansedContent;
    private String model;
    private EnrichmentContext context;
    private int totalItems;

    // No-arg constructor for Jackson deserialization
    public SQSMessage() {
    }

    public SQSMessage(String jobId, UUID cleansedDataStoreId, String sourcePath, String originalFieldName, String cleansedContent, String model, EnrichmentContext context, int totalItems) {
        this.jobId = jobId;
        this.cleansedDataStoreId = cleansedDataStoreId;
        this.sourcePath = sourcePath;
        this.originalFieldName = originalFieldName;
        this.cleansedContent = cleansedContent;
        this.model = model;
        this.context = context;
        this.totalItems = totalItems;
    }

    // Getters and setters for all fields
    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public UUID getCleansedDataStoreId() {
        return cleansedDataStoreId;
    }

    public void setCleansedDataStoreId(UUID cleansedDataStoreId) {
        this.cleansedDataStoreId = cleansedDataStoreId;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getOriginalFieldName() {
        return originalFieldName;
    }

    public void setOriginalFieldName(String originalFieldName) {
        this.originalFieldName = originalFieldName;
    }

    public String getCleansedContent() {
        return cleansedContent;
    }

    public void setCleansedContent(String cleansedContent) {
        this.cleansedContent = cleansedContent;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public EnrichmentContext getContext() {
        return context;
    }

    public void setContext(EnrichmentContext context) {
        this.context = context;
    }

    public int getTotalItems() {
        return totalItems;
    }

    public void setTotalItems(int totalItems) {
        this.totalItems = totalItems;
    }
}