package com.example.batchupload;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Spring Batch 6 / Java 25 bulk-load application.
 *
 * <h2>Quick start</h2>
 * <pre>
 * # Build
 * mvn -q clean package -DskipTests
 *
 * # Run (override file path and DB credentials)
 * java -jar target/batch-upload-1.0.0-SNAPSHOT.jar \
 *      --batch.file.path=/data/input.txt \
 *      --spring.datasource.url=jdbc:oracle:thin:@//db-host:1521/ORCL \
 *      --spring.datasource.username=batch_user \
 *      --spring.datasource.password=secret
 *
 * # Tune partitions and chunk size at runtime
 * java -jar target/batch-upload-1.0.0-SNAPSHOT.jar \
 *      --batch.file.path=/data/input.txt \
 *      --batch.partitions=32 \
 *      --batch.chunk-size=10000
 * </pre>
 *
 * <h2>Pre-requisite</h2>
 * Run {@code src/main/resources/schema-oracle.sql} once to create the
 * {@code DIMENSIONS} table and Spring Batch metadata tables.
 */
@SpringBootApplication
public class BatchUploadApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchUploadApplication.class, args);
    }
}
