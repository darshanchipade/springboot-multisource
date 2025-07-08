package com.apple.springboot.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "consolidated_enriched_sections")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsolidatedEnrichedSection {

    @Id
    @GeneratedValue
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "cleansed_data_id", nullable = false)
    private UUID cleansedDataId;

    @Column(name = "original_field_name")
    private String originalFieldName;

    @Type(type = "jsonb")
    @Column(name = "enrichment_metadata", columnDefinition = "jsonb")
    private String enrichmentMetadata;

    @Column(name = "enriched_at")
    private OffsetDateTime enrichedAt;

    @Column(name = "source_uri", columnDefinition = "TEXT")
    private String sourceUri;

    @Column(name = "section_path")
    private String sectionPath;

    @Column(name = "summary", columnDefinition = "TEXT")
    private String summary;

    @Column(name = "classification")
    private String classification;

    @Column(name = "tags", columnDefinition = "TEXT[]")
    private String[] tags;

    @Column(name = "keywords", columnDefinition = "TEXT[]")
    private String[] keywords;

    @Column(name = "cleansed_text", columnDefinition = "TEXT")
    private String cleansedText;

    @Column(name = "sentiment")
    private String sentiment;

    @Column(name = "section_uri", columnDefinition = "TEXT")
    private String sectionUri;

    @Column(name = "model_used")
    private String modelUsed;

    @Column(name = "status")
    private String status;

    @Column(name = "saved_at")
    private OffsetDateTime savedAt;

    // Getters and setters

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }



    public String getSourceUri() {
        return sourceUri;
    }

    public void setSourceUri(String sourceUri) {
        this.sourceUri = sourceUri;
    }
    public String getSectionUri() {
        return sectionUri;
    }

    public void setSectionUri(String sectionUri) {
        this.sectionUri = sectionUri;
    }

    public String getSectionPath() {
        return sectionPath;
    }

    public void setSectionPath(String sectionPath) {
        this.sectionPath = sectionPath;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public String getClassification() {
        return classification;
    }

    public void setClassification(String classification) {
        this.classification = classification;
    }

    public String[] getTags() {
        return tags;
    }

    public void setTags(String[] tags) {
        this.tags = tags;
    }

    public String[] getKeywords() {
        return keywords;
    }

    public void setKeywords(String[] keywords) {
        this.keywords = keywords;
    }

    public String getCleansedText() {
        return cleansedText;
    }

    public void setCleansedText(String cleansedText) {
        this.cleansedText = cleansedText;
    }

    public String getSentiment() {
        return sentiment;
    }

    public void setSentiment(String sentiment) {
        this.sentiment = sentiment;
    }

    public String getModelUsed() {
        return modelUsed;
    }

    public void setModelUsed(String modelUsed) {
        this.modelUsed = modelUsed;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public OffsetDateTime getSavedAt() {
        return savedAt;
    }

    public void setSavedAt(OffsetDateTime savedAt) {
        this.savedAt = savedAt;
    }
}
