package com.example.batchupload.config;

import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.model.FileRange;
import com.example.batchupload.partitioner.PodByteRangePartitioner;
import com.example.batchupload.processor.DimensionItemProcessor;
import com.example.batchupload.reader.ByteRangeFlatFileItemReader;
import com.example.batchupload.writer.DimensionItemWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Spring Batch configuration for the Dimension Load job.
 *
 * <h2>Architecture</h2>
 * <pre>
 *  Kubernetes Indexed Job
 *  ┌──────────────────────────────────────┐
 *  │  Pod 0  (JOB_COMPLETION_INDEX=0)     │
 *  │  ┌────────────────────────────────┐  │
 *  │  │ dimensionLoadJob               │  │
 *  │  │   managerStep                  │  │
 *  │  │     PodByteRangePartitioner    │  │
 *  │  │       ├─ workerStep-thread-0   │  │   ─┐
 *  │  │       ├─ workerStep-thread-1   │  │    │  threadPoolSize threads
 *  │  │       └─ workerStep-thread-N   │  │   ─┘
 *  │  └────────────────────────────────┘  │
 *  └──────────────────────────────────────┘
 *       ...repeated for Pod 1, 2, ..., N-1
 * </pre>
 *
 * <h2>Parallelism</h2>
 * <ul>
 *   <li><b>Cross-pod</b>: Each pod reads a distinct byte range of the shared file.
 *       {@code JOB_COMPLETION_INDEX} (0-based) and {@code TOTAL_PODS} drive the split.
 *   <li><b>Within-pod</b>: The pod's range is further divided into {@code threadPoolSize}
 *       sub-ranges, each handled by one worker thread.
 * </ul>
 *
 * <h2>Fault tolerance</h2>
 * All Spring Batch metadata (job/step execution state) is persisted in the shared Oracle
 * database, so a failed pod can be restarted without re-processing already-committed chunks.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final AppProperties props;
    private final DimensionItemProcessor processor;
    private final DimensionItemWriter writer;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    // ── K8s environment variables ──────────────────────────────────────────────

    @Value("${JOB_COMPLETION_INDEX:0}")
    private int podIndex;

    @Value("${TOTAL_PODS:1}")
    private int totalPods;

    // ── Job ───────────────────────────────────────────────────────────────────

    @Bean
    public Job dimensionLoadJob() {
        return new JobBuilder("dimensionLoadJob-pod" + podIndex, jobRepository)
                .start(managerStep())
                .build();
    }

    // ── Manager step (partitions & dispatches to worker threads) ──────────────

    @Bean
    public Step managerStep() {
        return new StepBuilder("managerStep", jobRepository)
                .partitioner("workerStep", podByteRangePartitioner())
                .partitionHandler(partitionHandler())
                .build();
    }

    @Bean
    public PodByteRangePartitioner podByteRangePartitioner() {
        Path filePath = Path.of(props.getFile().getPath());
        long fileSize = resolveFileSize(filePath);
        FileRange podRange = FileRange.forPod(fileSize, podIndex, totalPods);

        log.info("Pod {}/{} — file size={} bytes, range=[{}, {}), isLast={}",
                podIndex, totalPods, fileSize,
                podRange.startByte(), podRange.endByte(), podRange.isLast());

        return new PodByteRangePartitioner(filePath, podRange);
    }

    @Bean
    public TaskExecutorPartitionHandler partitionHandler() {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(workerStep());
        handler.setTaskExecutor(batchTaskExecutor());
        handler.setGridSize(props.getBatch().getThreadPoolSize());
        return handler;
    }

    // ── Worker step (runs inside each thread) ──────────────────────────────────

    @Bean
    public Step workerStep() {
        return new StepBuilder("workerStep", jobRepository)
                .<DimensionRecord, DimensionRecord>chunk(props.getBatch().getChunkSize(), transactionManager)
                // Reader is created per-partition via the factory method below
                .reader(workerStepReader(new ExecutionContext()))  // placeholder; real reader built in factory
                .processor(processor)
                .writer(writer)
                // Retry transient DB errors up to 3 times
                .faultTolerant()
                    .retryLimit(3)
                    .retry(org.springframework.dao.TransientDataAccessException.class)
                // Skip unrecoverable parse errors without failing the step
                    .skipLimit(10_000)
                    .skip(Exception.class)
                    .noSkip(org.springframework.dao.DataIntegrityViolationException.class)
                .build();
    }

    /**
     * Factory method called by Spring Batch for each partition's step execution.
     * The {@link ExecutionContext} carries {@code startByte}, {@code endByte},
     * and {@code isLast} populated by {@link PodByteRangePartitioner}.
     */
    public ByteRangeFlatFileItemReader workerStepReader(ExecutionContext ctx) {
        long startByte = ctx.containsKey("startByte") ? ctx.getLong("startByte") : 0L;
        long endByte = ctx.containsKey("endByte") ? ctx.getLong("endByte") : Long.MAX_VALUE;
        boolean isLast = !ctx.containsKey("isLast") || Boolean.parseBoolean(ctx.getString("isLast"));

        FileRange subRange = new FileRange(startByte, endByte, isLast);
        AppProperties.Columns cols = props.getColumns();

        return new ByteRangeFlatFileItemReader(
                Path.of(props.getFile().getPath()),
                subRange,
                cols.getCsiIdIndex(),
                cols.getPersonIdIndex(),
                cols.getCountryCodeIndex(),
                cols.getEconomicCodeIndex());
    }

    // ── Step-scoped reader bean wired via StepExecutionContext ─────────────────

    /**
     * This bean is step-scoped so Spring Batch creates a fresh instance per partition,
     * injecting the correct {@code stepExecutionContext} values.
     */
    @Bean
    @org.springframework.batch.core.configuration.annotation.StepScope
    public ByteRangeFlatFileItemReader stepScopedReader(
            @Value("#{stepExecutionContext['startByte'] ?: 0L}") long startByte,
            @Value("#{stepExecutionContext['endByte'] ?: 9223372036854775807L}") long endByte,
            @Value("#{stepExecutionContext['isLast'] ?: 'true'}") String isLast) {

        FileRange subRange = new FileRange(startByte, endByte, Boolean.parseBoolean(isLast));
        AppProperties.Columns cols = props.getColumns();

        return new ByteRangeFlatFileItemReader(
                Path.of(props.getFile().getPath()),
                subRange,
                cols.getCsiIdIndex(),
                cols.getPersonIdIndex(),
                cols.getCountryCodeIndex(),
                cols.getEconomicCodeIndex());
    }

    // ── Thread pool ───────────────────────────────────────────────────────────

    @Bean
    public TaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(props.getBatch().getThreadPoolSize());
        executor.setMaxPoolSize(props.getBatch().getThreadPoolSize());
        executor.setQueueCapacity(props.getBatch().getThreadPoolSize() * 2);
        executor.setThreadNamePrefix("batch-worker-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(600);
        executor.initialize();
        return executor;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static long resolveFileSize(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read file size for: " + path, e);
        }
    }
}
