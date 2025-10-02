package com.apple.springboot.service;

import com.apple.springboot.model.SQSMessage;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SqsEnrichmentPublisher {

    private static final Logger logger = LoggerFactory.getLogger(SqsEnrichmentPublisher.class);

    private final SqsTemplate sqsTemplate;
    private final String queueName;

    public SqsEnrichmentPublisher(SqsTemplate sqsTemplate, @Value("${sqs.queue.name}") String queueName) {
        this.sqsTemplate = sqsTemplate;
        this.queueName = queueName;
    }

    public void publishEnrichmentRequest(SQSMessage message) {
        try {
            sqsTemplate.send(to -> to.queue(queueName).payload(message));
            String sourcePath = message.getSourcePath();
            logger.info("Successfully published enrichment request for item: {}", sourcePath);
        } catch (Exception e) {
            String sourcePath = message.getSourcePath();
            logger.error("Failed to publish enrichment request for item: {}. Error: {}", sourcePath, e.getMessage(), e);
            // Re-throw exception to make the caller aware of the failure
            throw new RuntimeException("Failed to publish SQS message for sourcePath: " + sourcePath, e);
        }
    }
}