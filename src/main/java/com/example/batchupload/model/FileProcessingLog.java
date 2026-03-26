package com.example.batchupload.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "FILE_PROCESSING_LOG")
public class FileProcessingLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    private Long id;

    @Column(name = "FILE_NAME", nullable = false, length = 1024)
    private String fileName;

    @Column(name = "S3_BUCKET", length = 255)
    private String s3Bucket;

    @Column(name = "S3_KEY", nullable = false, length = 1024)
    private String s3Key;

    @Column(name = "FILE_SIZE")
    private Long fileSize;

    @Column(name = "STATUS", nullable = false, length = 20)
    private String status;

    @Column(name = "PICKED_BY_POD", nullable = false, length = 255)
    private String pickedByPod;

    @Column(name = "JOB_EXECUTION_ID")
    private Long jobExecutionId;

    @Column(name = "ROW_COUNT")
    private Long rowCount;

    @Column(name = "EXPECTED_ROW_COUNT")
    private Long expectedRowCount;

    @Column(name = "ERROR_MESSAGE", length = 4000)
    private String errorMessage;

    @Column(name = "STARTED_AT", nullable = false)
    private Instant startedAt;

    @Column(name = "FINISHED_AT")
    private Instant finishedAt;

    protected FileProcessingLog() {}

    public FileProcessingLog(String fileName, String s3Bucket, String s3Key,
                             long fileSize, String pickedByPod) {
        this.fileName = fileName;
        this.s3Bucket = s3Bucket;
        this.s3Key = s3Key;
        this.fileSize = fileSize;
        this.status = "PROCESSING";
        this.pickedByPod = pickedByPod;
        this.rowCount = 0L;
        this.startedAt = Instant.now();
    }

    public Long getId() { return id; }

    public String getFileName() { return fileName; }

    public String getS3Bucket() { return s3Bucket; }

    public String getS3Key() { return s3Key; }

    public Long getFileSize() { return fileSize; }

    public String getStatus() { return status; }

    public String getPickedByPod() { return pickedByPod; }

    public Long getJobExecutionId() { return jobExecutionId; }

    public Long getRowCount() { return rowCount; }

    public Long getExpectedRowCount() { return expectedRowCount; }

    public String getErrorMessage() { return errorMessage; }

    public Instant getStartedAt() { return startedAt; }

    public Instant getFinishedAt() { return finishedAt; }

    public void markCompleted(long jobExecutionId, long rowCount, Long expectedRowCount) {
        this.status = "COMPLETED";
        this.jobExecutionId = jobExecutionId;
        this.rowCount = rowCount;
        this.expectedRowCount = expectedRowCount;
        this.finishedAt = Instant.now();
    }

    public void markFailed(String errorMessage) {
        this.status = "FAILED";
        this.errorMessage = errorMessage != null && errorMessage.length() > 4000
                ? errorMessage.substring(0, 4000) : errorMessage;
        this.finishedAt = Instant.now();
    }
}
