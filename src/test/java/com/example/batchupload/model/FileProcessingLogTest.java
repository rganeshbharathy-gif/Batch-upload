package com.example.batchupload.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FileProcessingLogTest {

    @Test
    void constructor_setsInitialState() {
        FileProcessingLog log = new FileProcessingLog("file.dat", "bucket", "key/file.dat", 1024L, "pod-0");
        assertThat(log.getStatus()).isEqualTo("PROCESSING");
        assertThat(log.getRowCount()).isZero();
        assertThat(log.getStartedAt()).isNotNull();
        assertThat(log.getFinishedAt()).isNull();
        assertThat(log.getFileName()).isEqualTo("file.dat");
        assertThat(log.getS3Bucket()).isEqualTo("bucket");
        assertThat(log.getS3Key()).isEqualTo("key/file.dat");
        assertThat(log.getFileSize()).isEqualTo(1024L);
        assertThat(log.getPickedByPod()).isEqualTo("pod-0");
    }

    @Test
    void markCompleted_updatesAllFields() {
        FileProcessingLog log = new FileProcessingLog("f.dat", "b", "k", 100L, "pod-0");
        log.markCompleted(42L, 5000L, 5000L);

        assertThat(log.getStatus()).isEqualTo("COMPLETED");
        assertThat(log.getJobExecutionId()).isEqualTo(42L);
        assertThat(log.getRowCount()).isEqualTo(5000L);
        assertThat(log.getExpectedRowCount()).isEqualTo(5000L);
        assertThat(log.getFinishedAt()).isNotNull();
    }

    @Test
    void markFailed_setsStatusAndError() {
        FileProcessingLog log = new FileProcessingLog("f.dat", "b", "k", 100L, "pod-0");
        log.markFailed("Something went wrong");

        assertThat(log.getStatus()).isEqualTo("FAILED");
        assertThat(log.getErrorMessage()).isEqualTo("Something went wrong");
        assertThat(log.getFinishedAt()).isNotNull();
    }

    @Test
    void markFailed_truncatesLongErrorMessage() {
        FileProcessingLog log = new FileProcessingLog("f.dat", "b", "k", 100L, "pod-0");
        String longMessage = "x".repeat(5000);
        log.markFailed(longMessage);

        assertThat(log.getErrorMessage()).hasSize(4000);
    }
}
