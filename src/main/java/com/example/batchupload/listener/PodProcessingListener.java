package com.example.batchupload.listener;

import com.example.batchupload.model.PodProcessingLog;
import com.example.batchupload.repository.PodProcessingLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.listener.StepExecutionListener;

/**
 * Records pod processing metadata into {@code POD_PROCESSING_LOG} after each step completes.
 * Captures which pod processed which byte range, how many rows were read/written, and the status.
 *
 * <p>For the last pod, also stores the expected row count extracted from the file footer
 * so it can be compared against actual rows written across all pods.
 */
public class PodProcessingListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(PodProcessingListener.class);
    private static final String EXPECTED_ROW_COUNT_KEY = "footer.expectedRowCount";

    private final PodProcessingLogRepository podProcessingLogRepository;
    private final int podIndex;
    private final int totalPods;
    private final String bucket;
    private final String s3Key;
    private final long startByte;
    private final long endByte;

    public PodProcessingListener(PodProcessingLogRepository podProcessingLogRepository,
                                 int podIndex, int totalPods,
                                 String bucket, String s3Key,
                                 long startByte, long endByte) {
        this.podProcessingLogRepository = podProcessingLogRepository;
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

        // Read expected row count from execution context (set by last pod's reader)
        Long expectedRowCount = null;
        if (stepExecution.getExecutionContext().containsKey(EXPECTED_ROW_COUNT_KEY)) {
            expectedRowCount = stepExecution.getExecutionContext().getLong(EXPECTED_ROW_COUNT_KEY);
        }

        PodProcessingLog podLog = new PodProcessingLog(
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
                expectedRowCount,
                stepExecution.getStartTime().toInstant(),
                stepExecution.getEndTime().toInstant());

        podProcessingLogRepository.save(podLog);

        log.info("Pod {}/{} — wrote processing log: read={} written={} skipped={} status={} range=[{}, {}) expectedRowCount={}",
                podIndex, totalPods, readCount, writeCount, skipCount, status, startByte, endByte, expectedRowCount);
    }
}
