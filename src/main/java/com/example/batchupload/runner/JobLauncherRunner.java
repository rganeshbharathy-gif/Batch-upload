package com.example.batchupload.runner;

import com.example.batchupload.config.BatchProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.File;
import java.time.Duration;
import java.time.Instant;

/**
 * Triggers the batch job on application startup and shuts down the JVM when
 * done, making the jar suitable for invocation as a cron task or container job.
 *
 * <h2>Execution flow</h2>
 * <ol>
 *   <li>Validate the input file exists and is readable.</li>
 *   <li>Build {@link JobParameters} with the file path and current timestamp
 *       (the timestamp ensures each run is treated as a distinct job instance
 *       by Spring Batch, enabling re-runs without needing
 *       {@link org.springframework.batch.core.launch.support.RunIdIncrementer}
 *       explicitly).</li>
 *   <li>Launch the job synchronously (the default
 *       {@link org.springframework.batch.core.launch.support.TaskExecutorJobLauncher}
 *       waits for completion).</li>
 *   <li>Log throughput metrics and exit with code 0 (success) or 1 (failure).</li>
 * </ol>
 */
@Component
public class JobLauncherRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(JobLauncherRunner.class);

    private final JobLauncher    jobLauncher;
    private final Job            dimensionLoadJob;
    private final BatchProperties props;
    private final ApplicationContext context;

    public JobLauncherRunner(JobLauncher jobLauncher,
                             Job dimensionLoadJob,
                             BatchProperties props,
                             ApplicationContext context) {
        this.jobLauncher      = jobLauncher;
        this.dimensionLoadJob = dimensionLoadJob;
        this.props            = props;
        this.context          = context;
    }

    @Override
    public void run(String... args) throws Exception {
        String filePath = props.file().path();

        // ── Pre-flight check ────────────────────────────────────────────────
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            log.error("Input file not found: {}", filePath);
            shutdown(1);
            return;
        }
        if (!file.canRead()) {
            log.error("Input file is not readable: {}", filePath);
            shutdown(1);
            return;
        }

        long fileSizeGb = file.length() / (1024L * 1024L * 1024L);
        log.info("Starting dimensionLoadJob");
        log.info("  File         : {} ({} GB)", filePath, fileSizeGb);
        log.info("  Partitions   : {}", props.partitions());
        log.info("  Chunk size   : {}", props.chunkSize());
        log.info("  Columns      : csi_id={}, person_id={}, country_code={}, economic_code={}",
                props.columns().csiId(), props.columns().personId(),
                props.columns().countryCode(), props.columns().economicCode());

        // ── Job parameters ──────────────────────────────────────────────────
        // filePath is included so the same file can be reloaded with a new run.id
        JobParameters params = new JobParametersBuilder()
                .addString("filePath",  filePath)
                .addLong("startedAt",   System.currentTimeMillis())
                .toJobParameters();

        // ── Launch ──────────────────────────────────────────────────────────
        Instant start = Instant.now();
        JobExecution execution = jobLauncher.run(dimensionLoadJob, params);
        Duration elapsed = Duration.between(start, Instant.now());

        // ── Result ──────────────────────────────────────────────────────────
        long written = execution.getStepExecutions().stream()
                .mapToLong(se -> se.getWriteCount())
                .sum();

        switch (execution.getStatus()) {
            case COMPLETED -> {
                double rps = written / Math.max(elapsed.toSeconds(), 1);
                log.info("Job COMPLETED in {} – {} rows written ({} rows/s)",
                        formatDuration(elapsed), written, (long) rps);
                shutdown(0);
            }
            case FAILED -> {
                log.error("Job FAILED after {} – {} rows written. Check step executions for details.",
                        formatDuration(elapsed), written);
                execution.getAllFailureExceptions()
                        .forEach(ex -> log.error("Failure: ", ex));
                shutdown(1);
            }
            default -> {
                log.warn("Job ended with unexpected status {} after {}",
                        execution.getStatus(), formatDuration(elapsed));
                shutdown(1);
            }
        }
    }

    private void shutdown(int code) {
        SpringApplication.exit(context, (ExitCodeGenerator) () -> code);
    }

    private static String formatDuration(Duration d) {
        long h = d.toHours();
        long m = d.toMinutesPart();
        long s = d.toSecondsPart();
        return h > 0 ? "%dh %dm %ds".formatted(h, m, s)
                     : m > 0 ? "%dm %ds".formatted(m, s)
                              : "%ds".formatted(s);
    }
}
