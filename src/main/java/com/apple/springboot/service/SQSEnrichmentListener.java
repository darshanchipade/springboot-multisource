package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.EnrichmentMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;

@Service
public class SQSEnrichmentListener {

    private static final Logger logger = LoggerFactory.getLogger(SQSEnrichmentListener.class);

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final EnrichmentProcessor enrichmentProcessor;

    @Value("${aws.sqs.queue.url}")
    private String queueUrl;

    public SQSEnrichmentListener(SqsClient sqsClient, ObjectMapper objectMapper, EnrichmentProcessor enrichmentProcessor) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.enrichmentProcessor = enrichmentProcessor;
    }

    @Scheduled(fixedDelay = 5000) // Poll every 5 seconds
    public void pollQueue() {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    .build();

            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();

            for (Message message : messages) {
                try {
                    EnrichmentMessage enrichmentMessage = objectMapper.readValue(message.body(), EnrichmentMessage.class);
                    enrichmentProcessor.process(enrichmentMessage);
                    deleteMessage(message);
                } catch (Exception e) {
                    logger.error("Error processing message: " + message.body(), e);
                }
            }
        } catch (Exception e) {
            logger.error("Error polling SQS queue", e);
        }
    }

    private void deleteMessage(Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }
}