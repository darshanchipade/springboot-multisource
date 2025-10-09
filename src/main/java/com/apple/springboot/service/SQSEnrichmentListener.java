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

    @Scheduled(fixedDelay = 2000) // Poll frequently to keep throughput stable
    public void pollQueue() {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(5) // smaller batches reduce burstiness to Bedrock
                    .waitTimeSeconds(20)
                    .build();

            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();

            for (Message message : messages) {
                try {
                    EnrichmentMessage enrichmentMessage = objectMapper.readValue(message.body(), EnrichmentMessage.class);
                    enrichmentProcessor.process(enrichmentMessage);
                    // Only delete if processing completed without throttling
                    deleteMessage(message);
                } catch (Exception e) {
                    if (e instanceof ThrottledException) {
                        logger.warn("Throttled while processing message. Leaving it in-flight to retry after visibility timeout.");
                        // Do not delete; message will reappear after visibility timeout
                    } else {
                        logger.error("Error processing message: " + message.body(), e);
                        // Consider moving to DLQ based on retries; for now, leave it to be retried
                    }
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