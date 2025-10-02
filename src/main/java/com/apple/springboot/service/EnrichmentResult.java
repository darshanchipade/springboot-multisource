package com.apple.springboot.service;

import com.apple.springboot.model.EnrichedContentElement;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class EnrichmentResult {
    private final boolean success;
    private final boolean rateLimited;
    private final List<EnrichedContentElement> enrichedContentElements;
    private final String errorMessage;

    private EnrichmentResult(boolean success, boolean rateLimited, List<EnrichedContentElement> enrichedContentElements, String errorMessage) {
        this.success = success;
        this.rateLimited = rateLimited;
        this.enrichedContentElements = enrichedContentElements;
        this.errorMessage = errorMessage;
    }

    public static EnrichmentResult success(EnrichedContentElement enrichedContentElement) {
        return new EnrichmentResult(true, false, Collections.singletonList(enrichedContentElement), null);
    }

    public static EnrichmentResult success(List<EnrichedContentElement> enrichedContentElements) {
        return new EnrichmentResult(true, false, enrichedContentElements, null);
    }

    public static EnrichmentResult failure(String errorMessage) {
        return new EnrichmentResult(false, false, Collections.emptyList(), errorMessage);
    }

    public static EnrichmentResult rateLimited(String errorMessage) {
        return new EnrichmentResult(false, true, Collections.emptyList(), errorMessage);
    }

    public static EnrichmentResult skipped() {
        return new EnrichmentResult(false, false, Collections.emptyList(), "Skipped");
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean isFailure() {
        return !success && !rateLimited;
    }

    public boolean isRateLimited() {
        return rateLimited;
    }

    public List<EnrichedContentElement> getEnrichedContentElements() {
        return enrichedContentElements;
    }

    public Optional<String> getErrorMessage() {
        return Optional.ofNullable(errorMessage);
    }
}