package com.apple.springboot.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;

@Configuration
public class BedrockClientConfiguration {

    @Value("${aws.region}")
    private String awsRegion;

    @Bean
    public BedrockRuntimeClient bedrockRuntimeClient() {
        // DefaultCredentialsProvider searches for credentials in the standard chain:
        // 1. Environment Variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        // 2. Java System Properties (aws.accessKeyId, aws.secretKey)
        // 3. Credential profiles file (~/.aws/credentials)
        // 4. Credentials delivered through the Amazon EC2 metadata service
        // (if running on EC2)
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

        return BedrockRuntimeClient.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(credentialsProvider)
                .build();
    }
}