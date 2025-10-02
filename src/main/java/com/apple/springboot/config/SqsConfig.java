package com.apple.springboot.config;

import io.awspring.cloud.sqs.operations.SqsTemplate;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Configuration
public class SqsConfig {

    private static final Logger logger = LoggerFactory.getLogger(SqsConfig.class);

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${sqs.queue.name}")
    private String queueName;

    @Value("${sqs.endpoint.override:#{null}}")
    private URI endpointOverride;

    @Bean
    public SqsAsyncClient sqsAsyncClient() {
        SqsAsyncClient.Builder clientBuilder = SqsAsyncClient.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(DefaultCredentialsProvider.create());

        if (endpointOverride != null) {
            clientBuilder.endpointOverride(endpointOverride);
        }

        return clientBuilder.build();
    }

    @Bean
    public SqsTemplate sqsTemplate(SqsAsyncClient sqsAsyncClient) {
        return SqsTemplate.builder().sqsAsyncClient(sqsAsyncClient).build();
    }

    @PostConstruct
    public void createQueueIfNotExists() {
        try (SqsAsyncClient client = sqsAsyncClient()) {
            try {
                client.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).get();
                logger.info("SQS queue '{}' already exists.", queueName);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof SqsException && ((SqsException) e.getCause()).awsErrorDetails().errorCode().equals("AWS.SimpleQueueService.NonExistentQueue")) {
                    logger.info("SQS queue '{}' does not exist. Creating it...", queueName);
                    CreateQueueRequest request = CreateQueueRequest.builder()
                            .queueName(queueName)
                            .attributes(Map.of(
                                    QueueAttributeName.VISIBILITY_TIMEOUT, "300", // 5 minutes
                                    QueueAttributeName.MESSAGE_RETENTION_PERIOD, "86400" // 1 day
                            ))
                            .build();
                    client.createQueue(request).get();
                    logger.info("SQS queue '{}' created successfully.", queueName);
                } else {
                    throw new RuntimeException("Failed to check for SQS queue", e);
                }
            }
        } catch (Exception e) {
            logger.error("Error during SQS queue initialization", e);
            throw new RuntimeException("Could not initialize SQS queue", e);
        }
    }
}