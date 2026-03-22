package com.example.batchupload.runner;

import com.example.batchupload.config.BatchProperties;
import com.example.batchupload.lock.LockExtensionService;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Triggers the batch job on application startup with a distributed ShedLock
 * guard, ensuring that in multi-instance deployments only one node processes
 * the file at a time.
 *
 * <h2>Locking strategy</h2>
 * <pre>
 *  ┌─────────────────────────────────────────────────────────────────┐
 *  │  Node A                          Node B (concurrent start)      │
 *  │                                                                  │
 *  │  lockProvider.lock(cfg)          lockProvider.lock(cfg)         │
 *  │     → Optional[SimpleLock]          → Optional.empty()          │
 *  │                                      → skip, exit 0             │
 *  │  lockExtensionService.start()                                    │
 *  │  ┌─ every 10 min ──────────────┐                                │
 *  │  │  lock.extend(+30 min)       │                                │
 *  │  └─────────────────────────────┘                                │
 *  │  jobLauncher.run(job, params)                                    │
 *  │  lockExtensionService.stop()                                     │
 *  │  lock.unlock()                                                   │
 *  └─────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>Lock name</h2>
 * {@value #LOCK_NAME} – matches the job name so it is easy to correlate
 * in the {@code SHEDLOCK} table.
 *
 * <h2>Lock durations</h2>
 * <ul>
 *   <li>{@code lockAtMostFor} = {@code shedlock.extension-duration} (default 30 min).
 *       This is the initial "ceiling" at acquisition. The
 *       {@link LockExtensionService} then keeps pushing it forward every
 *       {@code shedlock.extension-interval} (default 10 min), so the
 *       effective ceiling is always 30 min in the future.</li>
 *   <li>{@code lockAtLeastFor} = {@code shedlock.lock-at-least-for} (default 5 min).
 *       Prevents a very fast run (or a run that crashes immediately) from
 *       releasing the lock so quickly that two instances overlap on a tight
 *       cron schedule.</li>
 * </ul>
 */
@Component
public class JobLauncherRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(JobLauncherRunner.class);

    /** Must be ≤ 64 characters (ShedLock table column constraint). */
    static final String LOCK_NAME = "dimensionLoadJob";

    private final JobLauncher           jobLauncher;
    private final Job                   dimensionLoadJob;
    private final BatchProperties       props;
    private final ApplicationContext    context;
    private final LockProvider          lockProvider;
    private final LockExtensionService  lockExtensionService;

    /** Initial lockAtMostFor at acquisition – same as one extension window. */
    private final Duration lockAtMostFor;

    /** Minimum lock hold time – prevents rapid double-execution. */
    private final Duration lockAtLeastFor;

    public JobLauncherRunner(
            JobLauncher jobLauncher,
            Job dimensionLoadJob,
            BatchProperties props,
            ApplicationContext context,
            LockProvider lockProvider,
            LockExtensionService lockExtensionService,
            @Value("${shedlock.extension-duration:PT30M}") Duration lockAtMostFor,
            @Value("${shedlock.lock-at-least-for:PT5M}")   Duration lockAtLeastFor) {
        this.jobLauncher          = jobLauncher;
        this.dimensionLoadJob     = dimensionLoadJob;
        this.props                = props;
        this.context              = context;
        this.lockProvider         = lockProvider;
        this.lockExtensionService = lockExtensionService;
        this.lockAtMostFor        = lockAtMostFor;
        this.lockAtLeastFor       = lockAtLeastFor;
    }

    @Override
    public void run(String... args) throws Exception {

        // ── 1. Pre-flight file check ─────────────────────────────────────────
        String filePath = props.file().path();
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

        // ── 2. Acquire distributed lock ──────────────────────────────────────
        LockConfiguration lockConfig = new LockConfiguration(
                Instant.now(),
                LOCK_NAME,
                lockAtMostFor,   // initial ceiling; extended periodically
                lockAtLeastFor   // minimum hold even if job finishes fast
        );

        Optional<SimpleLock> lockOpt = lockProvider.lock(lockConfig);

        if (lockOpt.isEmpty()) {
            // Another instance already holds the lock – this is expected in a
            // multi-pod deployment. Exit cleanly (code 0) so the orchestrator
            // does not treat this as an error.
            log.warn("ShedLock '{}' already held by another instance – skipping this run", LOCK_NAME);
            shutdown(0);
            return;
        }

        SimpleLock lock = lockOpt.get();
        log.info("ShedLock '{}' acquired (lockAtMostFor={}, lockAtLeastFor={})",
                LOCK_NAME, lockAtMostFor, lockAtLeastFor);

        // ── 3. Start rolling lock extension ─────────────────────────────────
        // The extension service renews the lock BEFORE it expires, so the
        // effective lockAtMostFor is always (extensionDuration) in the future.
        lockExtensionService.start(lock);

        // ── 4. Run the batch job ─────────────────────────────────────────────
        long fileSizeGb = file.length() / (1024L * 1024L * 1024L);
        log.info("Starting dimensionLoadJob");
        log.info("  File       : {} ({} GB)", filePath, fileSizeGb);
        log.info("  Partitions : {}", props.partitions());
        log.info("  Chunk size : {}", props.chunkSize());
        log.info("  Columns    : csi_id={}, person_id={}, country_code={}, economic_code={}",
                props.columns().csiId(), props.columns().personId(),
                props.columns().countryCode(), props.columns().economicCode());

        JobParameters params = new JobParametersBuilder()
                .addString("filePath",  filePath)
                .addLong("startedAt",   System.currentTimeMillis())
                .toJobParameters();

        int exitCode = 1;
        Instant start = Instant.now();

        try {
            JobExecution execution = jobLauncher.run(dimensionLoadJob, params);
            Duration elapsed = Duration.between(start, Instant.now());

            long written = execution.getStepExecutions().stream()
                    .mapToLong(se -> se.getWriteCount())
                    .sum();

            switch (execution.getStatus()) {
                case COMPLETED -> {
                    double rps = written / Math.max(elapsed.toSeconds(), 1);
                    log.info("Job COMPLETED in {} – {} rows written ({} rows/s)",
                            formatDuration(elapsed), written, (long) rps);
                    exitCode = 0;
                }
                case FAILED -> {
                    log.error("Job FAILED after {} – {} rows written", formatDuration(elapsed), written);
                    execution.getAllFailureExceptions()
                            .forEach(ex -> log.error("Failure: ", ex));
                }
                default ->
                    log.warn("Job ended with unexpected status {} after {}",
                            execution.getStatus(), formatDuration(elapsed));
            }

        } finally {
            // ── 5. Stop extension and release lock ───────────────────────────
            // Always executed – even if the job throws an unexpected exception.
            lockExtensionService.stop();
            lockExtensionService.unlockCurrent();
        }

        shutdown(exitCode);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

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
