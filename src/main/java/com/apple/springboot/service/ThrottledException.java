package com.apple.springboot.service;

/**
 * Signals a transient throttling condition (HTTP 429 / throughput exceeded).
 * Callers should NOT treat this as a permanent failure and should retry later.
 */
public class ThrottledException extends RuntimeException {
    public ThrottledException(String message) {
        super(message);
    }

    public ThrottledException(String message, Throwable cause) {
        super(message, cause);
    }
}
