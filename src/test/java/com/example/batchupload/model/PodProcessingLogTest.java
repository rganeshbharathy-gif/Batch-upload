package com.example.batchupload.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class PodProcessingLogTest {

    @Test
    void constructor_setsAllFields() {
        Instant start = Instant.now().minusSeconds(60);
        Instant end = Instant.now();

        PodProcessingLog log = new PodProcessingLog(
                100L, 2, 5,
                "bucket", "key/file.dat",
                1000L, 5000L,
                200L, 195L, 5L,
                "COMPLETED", 10000L,
                start, end);

        assertThat(log.getJobExecutionId()).isEqualTo(100L);
        assertThat(log.getPodIndex()).isEqualTo(2);
        assertThat(log.getTotalPods()).isEqualTo(5);
        assertThat(log.getS3Bucket()).isEqualTo("bucket");
        assertThat(log.getS3Key()).isEqualTo("key/file.dat");
        assertThat(log.getStartByte()).isEqualTo(1000L);
        assertThat(log.getEndByte()).isEqualTo(5000L);
        assertThat(log.getReadCount()).isEqualTo(200L);
        assertThat(log.getWriteCount()).isEqualTo(195L);
        assertThat(log.getSkipCount()).isEqualTo(5L);
        assertThat(log.getStatus()).isEqualTo("COMPLETED");
        assertThat(log.getExpectedRowCount()).isEqualTo(10000L);
        assertThat(log.getStartedAt()).isEqualTo(start);
        assertThat(log.getFinishedAt()).isEqualTo(end);
    }

    @Test
    void protectedConstructor_existsForJpa() {
        // Verify JPA no-arg constructor is accessible via reflection
        assertThat(PodProcessingLog.class.getDeclaredConstructors())
                .anyMatch(c -> c.getParameterCount() == 0);
    }
}
