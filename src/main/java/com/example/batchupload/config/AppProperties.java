package com.example.batchupload.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Immutable, type-safe configuration bound from {@code application.yml} under the
 * {@code app} prefix. All values can be overridden via environment variables using
 * Spring Boot's relaxed binding (e.g. {@code APP_S3_BUCKET}, {@code APP_COLUMNS_CSI_ID_INDEX}).
 */
@ConfigurationProperties(prefix = "app")
public record AppProperties(S3 s3, Batch batch) {

    public record S3(String bucket, String key, String region) {
        public S3 {
            if (region == null) region = "us-east-1";
        }
    }

    /**
     * @param chunkSize      rows per JDBC batch insert (optimal for Oracle: 2000-10000)
     * @param threadPoolSize number of parallel writer threads within this pod
     */
    public record Batch(int chunkSize, int threadPoolSize) {
        public Batch {
            if (chunkSize <= 0) chunkSize = 5000;
            if (threadPoolSize <= 0) threadPoolSize = 4;
        }
    }
}
