package com.apple.springboot.config;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimiterConfig {

    private final RateLimiterRegistry rateLimiterRegistry;

    public RateLimiterConfig(RateLimiterRegistry rateLimiterRegistry) {
        this.rateLimiterRegistry = rateLimiterRegistry;
    }

    @Bean
    public RateLimiter bedrockEnricherRateLimiter() {
        return rateLimiterRegistry.rateLimiter("bedrockEnricher");
    }
}
