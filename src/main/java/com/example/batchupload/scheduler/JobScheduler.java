package com.example.batchupload.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class JobScheduler {

    private static final Logger log = LoggerFactory.getLogger(JobScheduler.class);

    private final JobLauncher jobLauncher;
    private final Job dimensionLoadJob;

    public JobScheduler(JobLauncher jobLauncher, Job dimensionLoadJob) {
        this.jobLauncher = jobLauncher;
        this.dimensionLoadJob = dimensionLoadJob;
    }

    @Scheduled(cron = "${batch.schedule.cron:0 0 2 * * *}")
    public void runDimensionLoadJob() {
        try {
            String podIndex = "0";
            String totalPods = "1";

            JobParameters params = new JobParametersBuilder()
                    .addString("pod.index", podIndex)
                    .addString("total.pods", totalPods)
                    .addString("s3.bucket", "my-bucket")
                    .addString("s3.key", "data/dimensions.dat")
                    .addString("run.id", UUID.randomUUID().toString())
                    .toJobParameters();

            log.info("Scheduler triggering dimension load — pod {}/{}", podIndex, totalPods);
            var execution = jobLauncher.run(dimensionLoadJob, params);
            log.info("Job finished with status: {}", execution.getStatus());
        } catch (Exception e) {
            log.error("Scheduled job execution failed", e);
        }
    }
}
