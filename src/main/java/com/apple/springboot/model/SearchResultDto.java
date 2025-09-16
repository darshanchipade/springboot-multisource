package com.apple.springboot.model;

public class SearchResultDto {
    private String cleansedText;
    private String sourceFieldName;
    private String sectionPath;
    private double score;

    public SearchResultDto(String cleansedText, String sourceFieldName, String sectionPath, double score) {
        this.cleansedText = cleansedText;
        this.sourceFieldName = sourceFieldName;
        this.sectionPath = sectionPath;
        this.score = score;
    }

    // Getters and setters
    public String getCleansedText() {
        return cleansedText;
    }

    public void setCleansedText(String cleansedText) {
        this.cleansedText = cleansedText;
    }

    public String getSourceFieldName() {
        return sourceFieldName;
    }

    public void setSourceFieldName(String sourceFieldName) {
        this.sourceFieldName = sourceFieldName;
    }

    public String getSectionPath() {
        return sectionPath;
    }

    public void setSectionPath(String sectionPath) {
        this.sectionPath = sectionPath;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
