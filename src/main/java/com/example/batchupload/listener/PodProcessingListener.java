package com.example.batchupload.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.listener.StepExecutionListener;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Records pod processing metadata into {@code POD_PROCESSING_LOG} after each step completes.
 * Captures which pod processed which byte range, how many rows were read/written, and the status.
 */
public class PodProcessingListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(PodProcessingListener.class);

    private static final String INSERT_SQL =
            "INSERT INTO pod_processing_log "
            + "(job_execution_id, pod_index, total_pods, s3_bucket, s3_key, "
            + " start_byte, end_byte, read_count, write_count, skip_count, "
            + " status, started_at, finished_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final JdbcTemplate jdbcTemplate;
    private final int podIndex;
    private final int totalPods;
    private final String bucket;
    private final String s3Key;
    private final long startByte;
    private final long endByte;

    public PodProcessingListener(JdbcTemplate jdbcTemplate,
                                 int podIndex, int totalPods,
                                 String bucket, String s3Key,
                                 long startByte, long endByte) {
        this.jdbcTemplate = jdbcTemplate;
        this.podIndex = podIndex;
        this.totalPods = totalPods;
        this.bucket = bucket;
        this.s3Key = s3Key;
        this.startByte = startByte;
        this.endByte = endByte;
    }

    @Override
    public void afterStep(StepExecution stepExecution) {
        long readCount = stepExecution.getReadCount();
        long writeCount = stepExecution.getWriteCount();
        long skipCount = stepExecution.getReadSkipCount()
                + stepExecution.getWriteSkipCount()
                + stepExecution.getProcessSkipCount();
        String status = stepExecution.getStatus().name();

        jdbcTemplate.update(INSERT_SQL,
                stepExecution.getJobExecutionId(),
                podIndex,
                totalPods,
                bucket,
                s3Key,
                startByte,
                endByte,
                readCount,
                writeCount,
                skipCount,
                status,
                java.sql.Timestamp.valueOf(stepExecution.getStartTime()),
                java.sql.Timestamp.valueOf(stepExecution.getEndTime()));

        log.info("Pod {}/{} — wrote processing log: read={} written={} skipped={} status={} range=[{}, {})",
                podIndex, totalPods, readCount, writeCount, skipCount, status, startByte, endByte);
    }
}
