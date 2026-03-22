package com.example.batchupload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.UUID;

@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
public class BatchUploadApplication implements ApplicationRunner {

    private final JobLauncher jobLauncher;
    private final Job dimensionLoadJob;

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
                .addString("run.id", UUID.randomUUID().toString())  // ensures unique job instance
                .toJobParameters();

        log.info("Starting dimension load — pod {}/{}", podIndex, totalPods);
        var execution = jobLauncher.run(dimensionLoadJob, params);
        log.info("Job finished with status: {}", execution.getStatus());
    }
}
