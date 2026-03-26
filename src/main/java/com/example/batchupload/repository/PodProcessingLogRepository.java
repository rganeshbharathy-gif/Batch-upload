package com.example.batchupload.repository;

import com.example.batchupload.model.PodProcessingLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface PodProcessingLogRepository extends JpaRepository<PodProcessingLog, Long> {

    List<PodProcessingLog> findByJobExecutionId(Long jobExecutionId);

    @Query("SELECT SUM(p.writeCount) FROM PodProcessingLog p WHERE p.jobExecutionId = :jobExecutionId")
    Long sumWriteCountByJobExecutionId(Long jobExecutionId);

    @Query("SELECT p.expectedRowCount FROM PodProcessingLog p WHERE p.jobExecutionId = :jobExecutionId AND p.expectedRowCount IS NOT NULL")
    Long findExpectedRowCountByJobExecutionId(Long jobExecutionId);
}
