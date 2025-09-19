package com.apple.springboot.service;

import java.util.Optional;

public class EnrichmentResult {
    private final Status status;
    private final String errorMessage;

    private enum Status {
        SUCCESS,
        FAILURE,
        RATE_LIMITED,
        SKIPPED
    }

    private EnrichmentResult(Status status, String errorMessage) {
        this.status = status;
        this.errorMessage = errorMessage;
    }

    public static EnrichmentResult success() {
        return new EnrichmentResult(Status.SUCCESS, null);
    }

    public static EnrichmentResult failure(String errorMessage) {
        return new EnrichmentResult(Status.FAILURE, errorMessage);
    }

    public static EnrichmentResult rateLimited(String errorMessage) {
        return new EnrichmentResult(Status.RATE_LIMITED, errorMessage);
    }

    public static EnrichmentResult skipped() {
        return new EnrichmentResult(Status.SKIPPED, null);
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
}