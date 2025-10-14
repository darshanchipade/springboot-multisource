package com.apple.springboot.service;

import com.apple.springboot.model.EnrichmentMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
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
    private final TaskExecutor taskExecutor;

    @Value("${aws.sqs.queue.url}")
    private String queueUrl;

    public SQSEnrichmentListener(SqsClient sqsClient,
                                 ObjectMapper objectMapper,
                                 EnrichmentProcessor enrichmentProcessor,
                                 @Qualifier("sqsMessageProcessorExecutor") TaskExecutor taskExecutor) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.enrichmentProcessor = enrichmentProcessor;
        this.taskExecutor = taskExecutor;
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
                taskExecutor.execute(() -> processMessage(message));
            }
        } catch (Exception e) {
            logger.error("Error polling SQS queue", e);
        }
    }

    private void processMessage(Message message) {
        try {
            EnrichmentMessage enrichmentMessage = objectMapper.readValue(message.body(), EnrichmentMessage.class);
            enrichmentProcessor.process(enrichmentMessage);
            deleteMessage(message);
        } catch (ThrottledException te) {
            int delaySec = 180; // tune 120–300s
            try {
                sqsClient.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .visibilityTimeout(delaySec)
                        .build());
                logger.warn("Throttled; extended visibility {}s for message {} (will retry later).", delaySec, message.messageId());
            } catch (Exception visErr) {
                logger.error("Failed to change visibility for message {}: {}", message.messageId(), visErr.getMessage(), visErr);
            }
            // do NOT delete; allow redelivery
        } catch (Exception e) {
            logger.error("Error processing message {}: {}", message.messageId(), e.getMessage(), e);
            // leave for retry/DLQ
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