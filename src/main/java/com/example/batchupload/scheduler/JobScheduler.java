package com.example.batchupload.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
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

    @Value("${batch.pod.index}")
    private int podIndex;

    @Value("${batch.pod.total}")
    private int totalPods;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${aws.s3.key}")
    private String s3Key;

    public JobScheduler(JobLauncher jobLauncher, Job dimensionLoadJob) {
        this.jobLauncher = jobLauncher;
        this.dimensionLoadJob = dimensionLoadJob;
    }

    @Scheduled(cron = "${batch.schedule.cron:0 0 2 * * *}")
    public void runDimensionLoadJob() {
        try {
            JobParameters params = new JobParametersBuilder()
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
        } catch (Exception e) {
            log.error("Scheduled job execution failed", e);
        }
    }
}
