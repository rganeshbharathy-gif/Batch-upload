package com.example.batchupload;

import com.example.batchupload.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.UUID;

/**
 * Entry point. Each Kubernetes pod runs this independently; {@code JOB_COMPLETION_INDEX}
 * is injected by the K8s Indexed Job and steers the pod's byte range. A random
 * {@code run.id} guarantees a unique Spring Batch job instance per pod.
 */
@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
public class BatchUploadApplication implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(BatchUploadApplication.class);

    private final JobLauncher jobLauncher;
    private final Job dimensionLoadJob;

    public BatchUploadApplication(JobLauncher jobLauncher, Job dimensionLoadJob) {
        this.jobLauncher = jobLauncher;
        this.dimensionLoadJob = dimensionLoadJob;
    }

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(BatchUploadApplication.class, args)));
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String podIndex = System.getenv().getOrDefault("JOB_COMPLETION_INDEX", "0");
        String totalPods = System.getenv().getOrDefault("TOTAL_PODS", "1");

        JobParameters params = new JobParametersBuilder()
                .addString("pod.index", podIndex)
                .addString("total.pods", totalPods)
                .addString("run.id", UUID.randomUUID().toString())
                .toJobParameters();

        log.info("Starting dimension load — pod {}/{}", podIndex, totalPods);
        var execution = jobLauncher.run(dimensionLoadJob, params);
        log.info("Job finished with status: {}", execution.getStatus());
    }
}
