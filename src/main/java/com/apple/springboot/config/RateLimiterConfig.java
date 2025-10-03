package com.apple.springboot.config;

import com.google.common.util.concurrent.RateLimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimiterConfig {

    @Bean
    @SuppressWarnings("UnstableApiUsage")
    public RateLimiter bedrockRateLimiter() {
        // Allow 1 permit per second
        return RateLimiter.create(1.0);
    }
}