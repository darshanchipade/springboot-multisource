package com.apple.springboot.service;

import com.apple.springboot.model.EnrichedContentElement;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class EnrichmentResult {
    private final Status status;
    private final String errorMessage;
    private final List<EnrichedContentElement> enrichedContentElements;

    private enum Status {
        SUCCESS,
        FAILURE,
        RATE_LIMITED,
        SKIPPED
    }

    private EnrichmentResult(Status status, String errorMessage, List<EnrichedContentElement> elements) {
        this.status = status;
        this.errorMessage = errorMessage;
        this.enrichedContentElements = (elements != null) ? elements : Collections.emptyList();
    }

    public static EnrichmentResult success(EnrichedContentElement element) {
        return new EnrichmentResult(Status.SUCCESS, null, Collections.singletonList(element));
    }

    public static EnrichmentResult success(List<EnrichedContentElement> elements) {
        return new EnrichmentResult(Status.SUCCESS, null, elements);
    }

    public static EnrichmentResult failure(String errorMessage) {
        return new EnrichmentResult(Status.FAILURE, errorMessage, null);
    }

    public static EnrichmentResult rateLimited(String errorMessage) {
        return new EnrichmentResult(Status.RATE_LIMITED, errorMessage, null);
    }

    public static EnrichmentResult skipped() {
        return new EnrichmentResult(Status.SKIPPED, null, null);
    }

    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }

    public boolean isFailure() {
        return status == Status.FAILURE;
    }

    public boolean isRateLimited() {
        return status == Status.RATE_LIMITED;
    }

    public Optional<String> getErrorMessage() {
        return Optional.ofNullable(errorMessage);
    }

    public List<EnrichedContentElement> getEnrichedContentElements() {
        return enrichedContentElements;
    }
}