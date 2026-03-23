package com.example.batchupload.scheduler;

import com.example.batchupload.repository.FileProcessingRepository;
import com.example.batchupload.service.S3FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class JobScheduler {

    private static final Logger log = LoggerFactory.getLogger(JobScheduler.class);

    private final JobLauncher jobLauncher;
    private final Job dimensionLoadJob;
    private final FileProcessingRepository fileProcessingRepository;
    private final S3FileService s3FileService;

    @Value("${batch.pod.index}")
    private int podIndex;

    @Value("${batch.pod.total}")
    private int totalPods;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${aws.s3.key}")
    private String s3Key;

    public JobScheduler(JobLauncher jobLauncher,
                        Job dimensionLoadJob,
                        FileProcessingRepository fileProcessingRepository,
                        S3FileService s3FileService) {
        this.jobLauncher = jobLauncher;
        this.dimensionLoadJob = dimensionLoadJob;
        this.fileProcessingRepository = fileProcessingRepository;
        this.s3FileService = s3FileService;
    }

    @Scheduled(cron = "${batch.schedule.cron:0 0 2 * * *}")
    public void runDimensionLoadJob() {
        // 1. Check if this file has already been claimed
        if (fileProcessingRepository.isAlreadyClaimed(s3Bucket, s3Key)) {
            log.info("File already processed or in progress — skipping: bucket={} key={}", s3Bucket, s3Key);
            return;
        }

        // 2. Claim the file
        long fileSize = s3FileService.getFileSize(s3Bucket, s3Key);
        String fileName = s3Key.substring(s3Key.lastIndexOf('/') + 1);
        String podName = "pod-" + podIndex;
        long fileLogId;
        try {
            fileLogId = fileProcessingRepository.claim(s3Bucket, s3Key, fileName, fileSize, podName);
            log.info("Claimed file: bucket={} key={} fileLogId={}", s3Bucket, s3Key, fileLogId);
        } catch (Exception e) {
            log.info("Another pod already claimed this file — skipping: bucket={} key={}", s3Bucket, s3Key);
            return;
        }

        // 3. Launch the job
        try {
            var params = new JobParametersBuilder()
                    .addString("pod.index", String.valueOf(podIndex))
                    .addString("total.pods", String.valueOf(totalPods))
                    .addString("s3.bucket", s3Bucket)
                    .addString("s3.key", s3Key)
                    .addString("run.id", UUID.randomUUID().toString())
                    .toJobParameters();

            log.info("Scheduler triggering dimension load — pod {}/{} bucket={} key={}",
                    podIndex, totalPods, s3Bucket, s3Key);
            var execution = jobLauncher.run(dimensionLoadJob, params);
            log.info("Job finished with status: {}", execution.getStatus());

            // 4. Mark completed
            long rowCount = execution.getStepExecutions().stream()
                    .mapToLong(step -> step.getWriteCount())
                    .sum();
            fileProcessingRepository.markCompleted(fileLogId, execution.getId(), rowCount);

        } catch (Exception e) {
            log.error("Scheduled job execution failed", e);
            fileProcessingRepository.markFailed(fileLogId, e.getMessage());
        }
    }
}
