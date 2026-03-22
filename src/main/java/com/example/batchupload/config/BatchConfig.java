package com.example.batchupload.config;

import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.partitioner.FileRangePartitioner;
import com.example.batchupload.reader.PartitionedFileItemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Spring Batch 6 job configuration for loading a large pipe-delimited file
 * into the Oracle {@code DIMENSIONS} table.
 *
 * <h2>Architecture</h2>
 * <pre>
 *  dimensionLoadJob
 *    └─ masterStep  (partitioned, non-transactional coordinator)
 *         ├─ FileRangePartitioner  → N ExecutionContexts with [startOffset, endOffset)
 *         └─ TaskExecutorPartitionHandler
 *              └─ workerStep × N  (each on its own virtual thread)
 *                   ├─ PartitionedFileItemReader  (8 MB buffered, single-pass column extractor)
 *                   └─ JdbcBatchItemWriter        (JDBC batch INSERT, chunk-size rows / commit)
 * </pre>
 *
 * <h2>Virtual threads (Java 21+ / Java 25)</h2>
 * {@link SimpleAsyncTaskExecutor#setVirtualThreads(boolean)} (Spring Framework 6.1+)
 * routes every worker step onto a JVM virtual thread. Virtual threads are
 * ideal here because each worker is <em>I/O bound</em>: reading from disk and
 * writing to the database. The JVM scheduler pins carrier threads only during
 * CPU work, keeping the thread pool lean regardless of partition count.
 *
 * <h2>Chunk size tuning</h2>
 * The default chunk size is {@code 5000}. Each chunk is a single DB
 * transaction: {@code 5000} rows per {@code executeBatch()} call on the
 * Oracle JDBC driver. Increase for faster loads (fewer round-trips) or
 * decrease to reduce memory footprint per worker.
 *
 * <h2>Connection pool sizing</h2>
 * HikariCP {@code maximum-pool-size} should equal {@code batch.partitions}
 * so every virtual thread has a dedicated connection and never blocks waiting
 * for one.
 */
@Configuration
@EnableConfigurationProperties(BatchProperties.class)
public class BatchConfig {

    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

    // ── Job ───────────────────────────────────────────────────────────────────

    /**
     * Top-level job. {@link RunIdIncrementer} lets you re-run the same job
     * for the same file by supplying a new {@code run.id} parameter, which
     * is useful during development or for re-processing.
     */
    @Bean
    public Job dimensionLoadJob(JobRepository jobRepository, Step masterStep) {
        return new JobBuilder("dimensionLoadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(masterStep)
                .build();
    }

    // ── Master step (coordinator) ─────────────────────────────────────────────

    /**
     * Partitioned master step.
     *
     * <p>{@link TaskExecutorPartitionHandler} is Spring Batch's built-in
     * <em>local</em> (in-process) partition handler. It submits each worker
     * step execution to the provided {@link TaskExecutor} – in this case a
     * virtual-thread executor, so the JVM creates one lightweight virtual
     * thread per partition.
     *
     * <p>{@code gridSize} controls how many partitions the file is divided
     * into. It is read from {@code batch.partitions} and defaults to 20.
     */
    @Bean
    public Step masterStep(JobRepository jobRepository,
                           FileRangePartitioner fileRangePartitioner,
                           Step workerStep,
                           TaskExecutor virtualThreadTaskExecutor,
                           BatchProperties props) {

        TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();
        partitionHandler.setTaskExecutor(virtualThreadTaskExecutor);
        partitionHandler.setStep(workerStep);
        partitionHandler.setGridSize(props.partitions());

        return new StepBuilder("masterStep", jobRepository)
                .partitioner("workerStep", fileRangePartitioner)
                .partitionHandler(partitionHandler)
                .build();
    }

    // ── Worker step ───────────────────────────────────────────────────────────

    /**
     * The worker step executed once per partition.
     *
     * <p>Because {@link PartitionedFileItemReader} and
     * {@link JdbcBatchItemWriter} are both {@code @StepScope}, Spring
     * injects a dedicated instance for each step execution (partition).
     * No shared mutable state exists between virtual threads.
     */
    @Bean
    public Step workerStep(JobRepository jobRepository,
                           PlatformTransactionManager transactionManager,
                           PartitionedFileItemReader partitionedFileItemReader,
                           JdbcBatchItemWriter<DimensionRecord> dimensionItemWriter,
                           BatchProperties props) {

        return new StepBuilder("workerStep", jobRepository)
                .<DimensionRecord, DimensionRecord>chunk(props.chunkSize(), transactionManager)
                .reader(partitionedFileItemReader)
                .writer(dimensionItemWriter)
                .faultTolerant()
                // Skip malformed lines rather than failing the whole partition.
                .skipLimit(100)
                .skip(Exception.class)
                .build();
    }

    // ── Step-scoped reader (one instance per partition) ───────────────────────

    /**
     * Factory for a partition-aware item reader.
     *
     * <p>{@code @StepScope} ensures Spring creates a fresh
     * {@link PartitionedFileItemReader} for every step execution (partition).
     * The {@code startOffset} and {@code endOffset} are injected directly from
     * the partition's {@link org.springframework.batch.item.ExecutionContext}
     * via Spring EL expressions.
     */
    @Bean
    @org.springframework.batch.core.configuration.annotation.StepScope
    public PartitionedFileItemReader partitionedFileItemReader(
            @Value("#{stepExecutionContext['startOffset']}") Long startOffset,
            @Value("#{stepExecutionContext['endOffset']}")   Long endOffset,
            BatchProperties props) {

        log.debug("Creating reader for partition [{}, {})", startOffset, endOffset);
        return new PartitionedFileItemReader(
                props.file().path(),
                startOffset,
                endOffset,
                props.columns()
        );
    }

    // ── Step-scoped writer (one instance per partition) ───────────────────────

    /**
     * JDBC batch writer that inserts records into the {@code DIMENSIONS} table.
     *
     * <p>Named parameters ({@code :csiId} etc.) are resolved via
     * {@code BeanPropertySqlParameterSource}, which reads the Java record's
     * accessor methods ({@code csiId()}, {@code personId()}, …).
     *
     * <p>The {@code CREATED_AT} column is intentionally omitted – the Oracle
     * column default ({@code DEFAULT SYSTIMESTAMP}) fills it automatically,
     * which is both simpler and more accurate than setting it in Java.
     *
     * <p>{@code assertUpdates(false)} is set to avoid an exception when a row
     * is skipped by the fault-tolerant configuration above.
     */
    @Bean
    @org.springframework.batch.core.configuration.annotation.StepScope
    public JdbcBatchItemWriter<DimensionRecord> dimensionItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<DimensionRecord>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO dimensions (csi_id, person_id, country_code, economic_code)
                        VALUES (:csiId, :personId, :countryCode, :economicCode)
                        """)
                .beanMapped()
                .assertUpdates(false)
                .build();
    }

    // ── Partitioner ───────────────────────────────────────────────────────────

    @Bean
    public FileRangePartitioner fileRangePartitioner(BatchProperties props) {
        return new FileRangePartitioner(
                props.file().path(),
                props.file().hasHeader()
        );
    }

    // ── Virtual-thread task executor ─────────────────────────────────────────

    /**
     * Task executor backed by JVM virtual threads (Project Loom, Java 21+).
     *
     * <p>{@link SimpleAsyncTaskExecutor} with {@code setVirtualThreads(true)}
     * (Spring Framework 6.1 / Spring Boot 3.2+) creates a new virtual thread
     * for every submitted task. Virtual threads are cheap – the JVM can
     * sustain millions concurrently – so using one per partition is fine even
     * with a high {@code batch.partitions} value.
     *
     * <p>The thread name prefix {@code "batch-worker-"} makes partition threads
     * easy to identify in thread dumps and APM tools.
     */
    @Bean
    public TaskExecutor virtualThreadTaskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("batch-worker-");
        executor.setVirtualThreads(true);
        // No concurrency limit: each partition runs immediately, bounded only by
        // the HikariCP pool (set maximum-pool-size = batch.partitions).
        executor.setConcurrencyLimit(SimpleAsyncTaskExecutor.UNBOUNDED_CONCURRENCY);
        return executor;
    }
}
