package com.apple.springboot.config;

import com.google.common.util.concurrent.RateLimiter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimiterConfig {

    @Bean
    @SuppressWarnings("UnstableApiUsage")
    public RateLimiter bedrockChatRateLimiter(
            @Value("${app.ratelimit.chatQps:0.5}") double chatQps
    ) {
        // Limit for Bedrock chat/completion calls (TPS varies by account/model)
        return RateLimiter.create(Math.max(0.1, chatQps));
    }

    @Bean
    @SuppressWarnings("UnstableApiUsage")
    public RateLimiter bedrockEmbedRateLimiter(
            @Value("${app.ratelimit.embedQps:5.0}") double embedQps
    ) {
        // Separate limiter for embedding model calls
        return RateLimiter.create(Math.max(0.1, embedQps));
    }
}