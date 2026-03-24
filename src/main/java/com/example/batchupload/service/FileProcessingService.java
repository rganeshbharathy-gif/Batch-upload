package com.example.batchupload.service;

import com.example.batchupload.model.FileProcessingLog;
import com.example.batchupload.repository.FileProcessingLogRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Manages rows in {@code FILE_PROCESSING_LOG}.
 * Used by the scheduler to claim new S3 files and update their status after job completion.
 */
@Service
public class FileProcessingService {

    private final FileProcessingLogRepository fileProcessingLogRepository;

    public FileProcessingService(FileProcessingLogRepository fileProcessingLogRepository) {
        this.fileProcessingLogRepository = fileProcessingLogRepository;
    }

    /**
     * Returns {@code true} if the file has already been claimed (PROCESSING or COMPLETED).
     */
    public boolean isAlreadyClaimed(String bucket, String s3Key) {
        return fileProcessingLogRepository.existsByS3BucketAndS3KeyAndStatusIn(
                bucket, s3Key, List.of("PROCESSING", "COMPLETED"));
    }

    /**
     * Claims the file by inserting a PROCESSING row. Returns the generated ID.
     * If the unique constraint fires (another pod claimed it first), the caller
     * should catch the exception and skip the file.
     */
    @Transactional
    public long claim(String bucket, String s3Key, String fileName, long fileSize, String podName) {
        var log = new FileProcessingLog(fileName, bucket, s3Key, fileSize, podName);
        return fileProcessingLogRepository.save(log).getId();
    }

    /**
     * Marks the file as COMPLETED and records the job execution ID and row count.
     */
    @Transactional
    public void markCompleted(long id, long jobExecutionId, long rowCount) {
        var log = fileProcessingLogRepository.findById(id).orElseThrow(
                () -> new IllegalStateException("FileProcessingLog not found: " + id));
        log.markCompleted(jobExecutionId, rowCount);
        fileProcessingLogRepository.save(log);
    }

    /**
     * Marks the file as FAILED with an error message.
     */
    @Transactional
    public void markFailed(long id, String errorMessage) {
        var log = fileProcessingLogRepository.findById(id).orElseThrow(
                () -> new IllegalStateException("FileProcessingLog not found: " + id));
        log.markFailed(errorMessage);
        fileProcessingLogRepository.save(log);
    }
}
