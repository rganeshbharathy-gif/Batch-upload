package com.example.batchupload.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * Manages rows in {@code FILE_PROCESSING_LOG}.
 * Used by the scheduler to claim new S3 files and update their status after job completion.
 */
@Repository
public class FileProcessingRepository {

    private final JdbcTemplate jdbcTemplate;

    public FileProcessingRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Returns {@code true} if the file has already been claimed (PROCESSING or COMPLETED).
     */
    public boolean isAlreadyClaimed(String bucket, String s3Key) {
        String sql = "SELECT COUNT(*) FROM FILE_PROCESSING_LOG "
                + "WHERE S3_BUCKET = ? AND S3_KEY = ? AND STATUS IN ('PROCESSING', 'COMPLETED')";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, bucket, s3Key);
        return count != null && count > 0;
    }

    /**
     * Claims the file by inserting a PROCESSING row. Returns the generated ID.
     * If the unique constraint fires (another pod claimed it first), the caller
     * should catch the exception and skip the file.
     */
    public long claim(String bucket, String s3Key, String fileName, long fileSize, String podName) {
        String sql = "INSERT INTO FILE_PROCESSING_LOG "
                + "(FILE_NAME, S3_BUCKET, S3_KEY, FILE_SIZE, STATUS, PICKED_BY_POD, STARTED_AT) "
                + "VALUES (?, ?, ?, ?, 'PROCESSING', ?, ?)";
        jdbcTemplate.update(sql, fileName, bucket, s3Key, fileSize, podName,
                Timestamp.from(Instant.now()));

        return jdbcTemplate.queryForObject(
                "SELECT ID FROM FILE_PROCESSING_LOG WHERE S3_BUCKET = ? AND S3_KEY = ?",
                Long.class, bucket, s3Key);
    }

    /**
     * Marks the file as COMPLETED and records the job execution ID and row count.
     */
    public void markCompleted(long id, long jobExecutionId, long rowCount) {
        String sql = "UPDATE FILE_PROCESSING_LOG "
                + "SET STATUS = 'COMPLETED', JOB_EXECUTION_ID = ?, ROW_COUNT = ?, FINISHED_AT = ? "
                + "WHERE ID = ?";
        jdbcTemplate.update(sql, jobExecutionId, rowCount, Timestamp.from(Instant.now()), id);
    }

    /**
     * Marks the file as FAILED with an error message.
     */
    public void markFailed(long id, String errorMessage) {
        String sql = "UPDATE FILE_PROCESSING_LOG "
                + "SET STATUS = 'FAILED', ERROR_MESSAGE = ?, FINISHED_AT = ? "
                + "WHERE ID = ?";
        String truncated = errorMessage != null && errorMessage.length() > 4000
                ? errorMessage.substring(0, 4000) : errorMessage;
        jdbcTemplate.update(sql, truncated, Timestamp.from(Instant.now()), id);
    }
}
