package com.apple.springboot.model;

import lombok.Data;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class JobProgress {
    private final String jobId;
    private volatile String status;
    private final int totalItems;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final List<String> recentEvents = new ArrayList<>();
    private final SseEmitter emitter;

    public JobProgress(int totalItems) {
        this.jobId = UUID.randomUUID().toString();
        this.totalItems = totalItems;
        this.emitter = new SseEmitter(Long.MAX_VALUE);
        this.status = "PENDING";
    }

    public synchronized void addEvent(String event) {
        recentEvents.add(event);
        if (recentEvents.size() > 10) {
            recentEvents.remove(0);
        }
    }
}