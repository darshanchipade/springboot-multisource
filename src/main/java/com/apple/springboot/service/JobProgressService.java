package com.apple.springboot.service;

import com.apple.springboot.model.JobProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class JobProgressService {

    private static final Logger logger = LoggerFactory.getLogger(JobProgressService.class);
    private final Map<String, JobProgress> jobs = new ConcurrentHashMap<>();

    public JobProgress createJob(int totalItems) {
        JobProgress job = new JobProgress(totalItems);
        jobs.put(job.getJobId(), job);
        logger.info("Created new progress tracking job with ID: {}", job.getJobId());
        return job;
    }

    public SseEmitter getEmitter(String jobId) {
        JobProgress job = jobs.get(jobId);
        if (job != null) {
            return job.getEmitter();
        }
        return null;
    }

    public void updateProgress(String jobId, String eventMessage) {
        JobProgress job = jobs.get(jobId);
        if (job != null) {
            try {
                job.addEvent(eventMessage);
                job.getEmitter().send(SseEmitter.event().name("message").data(eventMessage));
            } catch (IOException e) {
                logger.warn("Failed to send SSE event for job ID: {}. Removing emitter.", jobId, e);
                job.getEmitter().complete();
                jobs.remove(jobId);
            }
        }
    }

    public void completeJob(String jobId) {
        JobProgress job = jobs.get(jobId);
        if (job != null) {
            try {
                job.getEmitter().send(SseEmitter.event().name("complete").data("Job Finished"));
            } catch (IOException e) {
                logger.warn("Failed to send completion event for job ID: {}", jobId, e);
            } finally {
                job.getEmitter().complete();
                jobs.remove(jobId);
                logger.info("Completed and removed job ID: {}", jobId);
            }
        }
    }
}