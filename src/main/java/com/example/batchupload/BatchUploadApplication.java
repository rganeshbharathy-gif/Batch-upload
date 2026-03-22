package com.example.batchupload;

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
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

import java.util.UUID;

@SpringBootApplication
@ConfigurationPropertiesScan
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
        /*
         * Each Kubernetes pod runs this independently.
         * JOB_COMPLETION_INDEX is injected by K8s Indexed Job (0-based).
         * A unique run.id prevents Spring Batch from treating this as
         * a duplicate job instance across pods.
         */
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
