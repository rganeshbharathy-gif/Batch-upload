package com.example.batchupload.config;

import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.model.FileLayout;
import com.example.batchupload.model.FileRange;
import com.example.batchupload.partitioner.PodByteRangePartitioner;
import com.example.batchupload.processor.DimensionItemProcessor;
import com.example.batchupload.reader.ByteRangeFlatFileItemReader;
import com.example.batchupload.reader.FileLayoutScanner;
import com.example.batchupload.writer.DimensionItemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;

/**
 * Spring Batch configuration for the daily Dimension Load job (Spring Batch 6).
 *
 * <h2>Lifecycle per JVM</h2>
 * <ol>
 *   <li>{@link FileLayoutScanner} reads the vendor metadata line, parses the header
 *       to resolve the four required column indices, and locates the footer row's
 *       byte offset. Only this region {@code [dataStart, dataEnd)} is partitioned.</li>
 *   <li>{@link FileRange#forPod} carves out this pod's slice of the data region.</li>
 *   <li>{@link PodByteRangePartitioner} divides the pod's slice across worker threads.</li>
 *   <li>Each worker thread runs the same {@code workerStep} with its own
 *       step-scoped {@link ByteRangeFlatFileItemReader}.</li>
 *   <li>{@link DimensionItemWriter} MERGEs each chunk into Oracle on {@code person_id}.</li>
 * </ol>
 */
@Configuration
public class BatchConfig {

    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

    private final AppProperties props;
    private final DimensionItemProcessor processor;
    private final DimensionItemWriter writer;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Value("${JOB_COMPLETION_INDEX:0}")
    private int podIndex;

    @Value("${TOTAL_PODS:1}")
    private int totalPods;

    public BatchConfig(AppProperties props,
                       DimensionItemProcessor processor,
                       DimensionItemWriter writer,
                       JobRepository jobRepository,
                       PlatformTransactionManager transactionManager) {
        this.props = props;
        this.processor = processor;
        this.writer = writer;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    // ── File layout (scanned once) ────────────────────────────────────────────

    @Bean
    public FileLayout fileLayout() throws IOException {
        Path path = Path.of(props.file().path());
        AppProperties.Columns c = props.columns();
        FileLayout layout = new FileLayoutScanner(
                path, c.gridId(), c.personId(), c.countryCode(), c.ecoSectorCode()).scan();
        log.info("File layout resolved — dataStart={} dataEnd={} dataLength={} columns={}",
                layout.dataStart(), layout.dataEnd(), layout.dataLength(), layout.columnIndex());
        return layout;
    }

    /** Captured once per JVM so every merged row shares one timestamp. */
    @Bean
    public Instant loadedAt(Clock clock) {
        return Instant.now(clock);
    }

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    // ── Job ───────────────────────────────────────────────────────────────────

    @Bean
    public Job dimensionLoadJob(Step managerStep) {
        return new JobBuilder("dimensionLoadJob-pod" + podIndex, jobRepository)
                .start(managerStep)
                .build();
    }

    // ── Manager step ──────────────────────────────────────────────────────────

    @Bean
    public Step managerStep(Step workerStep, FileLayout layout) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(workerStep);
        handler.setTaskExecutor(batchTaskExecutor());
        handler.setGridSize(props.batch().threadPoolSize());

        FileRange podRange = FileRange.forPod(
                layout.dataStart(), layout.dataEnd(), podIndex, totalPods);
        log.info("Pod {}/{} — range=[{}, {}) isLast={}",
                podIndex, totalPods, podRange.startByte(), podRange.endByte(), podRange.isLast());

        return new StepBuilder("managerStep", jobRepository)
                .partitioner("workerStep", new PodByteRangePartitioner(podRange))
                .partitionHandler(handler)
                .build();
    }

    // ── Worker step ───────────────────────────────────────────────────────────

    @Bean
    public Step workerStep(ByteRangeFlatFileItemReader stepScopedReader) {
        return new StepBuilder("workerStep", jobRepository)
                .<DimensionRecord, DimensionRecord>chunk(props.batch().chunkSize(), transactionManager)
                .reader(stepScopedReader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                    .retryLimit(3)
                    .retry(TransientDataAccessException.class)
                    .skipLimit(props.batch().skipLimit())
                    .skip(Exception.class)
                    .noSkip(DataIntegrityViolationException.class)
                .build();
    }

    @Bean
    @StepScope
    public ByteRangeFlatFileItemReader stepScopedReader(
            FileLayout layout,
            Instant loadedAt,
            @Value("#{stepExecutionContext['startByte']}") long startByte,
            @Value("#{stepExecutionContext['endByte']}") long endByte,
            @Value("#{stepExecutionContext['isLast']}") String isLast) {

        FileRange range = new FileRange(startByte, endByte, Boolean.parseBoolean(isLast));
        return new ByteRangeFlatFileItemReader(
                Path.of(props.file().path()),
                range,
                layout.dataStart(),
                layout.columnIndex(),
                loadedAt);
    }

    // ── Thread pool ───────────────────────────────────────────────────────────

    @Bean
    public TaskExecutor batchTaskExecutor() {
        int threads = props.batch().threadPoolSize();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threads);
        executor.setMaxPoolSize(threads);
        executor.setQueueCapacity(threads * 2);
        executor.setThreadNamePrefix("batch-worker-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(600);
        executor.initialize();
        return executor;
    }
}
