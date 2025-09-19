package com.apple.springboot.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class AsyncConfig {

    @Value("${enrichment.executor.core-pool-size:5}")
    private int corePoolSize;

    @Value("${enrichment.executor.max-pool-size:10}")
    private int maxPoolSize;

    @Value("${enrichment.executor.queue-capacity:25}")
    private int queueCapacity;

    @Bean("enrichmentTaskExecutor")
    public Executor enrichmentTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("Enrichment-");
        executor.initialize();
        return executor;
    }
}
