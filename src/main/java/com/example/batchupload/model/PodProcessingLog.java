package com.example.batchupload.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "POD_PROCESSING_LOG")
public class PodProcessingLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    private Long id;

    @Column(name = "JOB_EXECUTION_ID", nullable = false)
    private Long jobExecutionId;

    @Column(name = "POD_INDEX", nullable = false)
    private Integer podIndex;

    @Column(name = "TOTAL_PODS", nullable = false)
    private Integer totalPods;

    @Column(name = "S3_BUCKET", length = 255)
    private String s3Bucket;

    @Column(name = "S3_KEY", length = 1024)
    private String s3Key;

    @Column(name = "START_BYTE", nullable = false)
    private Long startByte;

    @Column(name = "END_BYTE", nullable = false)
    private Long endByte;

    @Column(name = "READ_COUNT")
    private Long readCount;

    @Column(name = "WRITE_COUNT")
    private Long writeCount;

    @Column(name = "SKIP_COUNT")
    private Long skipCount;

    @Column(name = "FILTER_COUNT")
    private Long filterCount;

    @Column(name = "STATUS", nullable = false, length = 20)
    private String status;

    @Column(name = "EXPECTED_ROW_COUNT")
    private Long expectedRowCount;

    @Column(name = "STARTED_AT")
    private Instant startedAt;

    @Column(name = "FINISHED_AT", nullable = false)
    private Instant finishedAt;

    protected PodProcessingLog() {}

    public PodProcessingLog(Long jobExecutionId, int podIndex, int totalPods,
                            String s3Bucket, String s3Key,
                            long startByte, long endByte,
                            long readCount, long writeCount, long skipCount,
                            long filterCount, String status, Long expectedRowCount,
                            Instant startedAt, Instant finishedAt) {
        this.jobExecutionId = jobExecutionId;
        this.podIndex = podIndex;
        this.totalPods = totalPods;
        this.s3Bucket = s3Bucket;
        this.s3Key = s3Key;
        this.startByte = startByte;
        this.endByte = endByte;
        this.readCount = readCount;
        this.writeCount = writeCount;
        this.skipCount = skipCount;
        this.filterCount = filterCount;
        this.status = status;
        this.expectedRowCount = expectedRowCount;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
    }

    public Long getId() { return id; }
    public Long getJobExecutionId() { return jobExecutionId; }
    public Integer getPodIndex() { return podIndex; }
    public Integer getTotalPods() { return totalPods; }
    public String getS3Bucket() { return s3Bucket; }
    public String getS3Key() { return s3Key; }
    public Long getStartByte() { return startByte; }
    public Long getEndByte() { return endByte; }
    public Long getReadCount() { return readCount; }
    public Long getWriteCount() { return writeCount; }
    public Long getSkipCount() { return skipCount; }
    public Long getFilterCount() { return filterCount; }
    public String getStatus() { return status; }
    public Long getExpectedRowCount() { return expectedRowCount; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getFinishedAt() { return finishedAt; }
}
