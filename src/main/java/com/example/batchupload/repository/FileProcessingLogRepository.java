package com.example.batchupload.repository;

import com.example.batchupload.model.FileProcessingLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface FileProcessingLogRepository extends JpaRepository<FileProcessingLog, Long> {

    boolean existsByS3BucketAndS3KeyAndStatusIn(String s3Bucket, String s3Key, List<String> statuses);

    void deleteByS3BucketAndS3KeyAndStatusIn(String s3Bucket, String s3Key, List<String> statuses);
}
