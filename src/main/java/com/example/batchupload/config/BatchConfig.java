package com.example.batchupload.config;

import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.model.FileRange;
import com.example.batchupload.processor.DimensionItemProcessor;
import com.example.batchupload.reader.ByteRangeFlatFileItemReader;
import com.example.batchupload.service.S3FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.item.ChunkProcessor;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.integration.chunk.ChunkTaskExecutorItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

/**
 * Spring Batch 6 configuration for the Dimension Load job using <b>local chunking</b>.
 *
 * <h2>Architecture (Local Chunking — Spring Batch 6)</h2>
 * <pre>
 *  Kubernetes Indexed Job
 *  ┌─────────────────────────────────────────────────────────┐
 *  │  Pod 0  (JOB_COMPLETION_INDEX=0)                        │
 *  │  ┌───────────────────────────────────────────────────┐  │
 *  │  │ dimensionLoadJob                                  │  │
 *  │  │   loadStep (producer-consumer local chunking)     │  │
 *  │  │                                                   │  │
 *  │  │   [Reader] ─→ [Processor] ─→ chunk queue ─→      │  │
 *  │  │     (single thread, sequential disk I/O)          │  │
 *  │  │                                 ├→ writer-0 (tx)  │  │
 *  │  │                                 ├→ writer-1 (tx)  │  │
 *  │  │                                 ├→ writer-2 (tx)  │  │
 *  │  │                                 └→ writer-3 (tx)  │  │
 *  │  │     (parallel DB writes, each in own transaction) │  │
 *  │  └───────────────────────────────────────────────────┘  │
 *  └─────────────────────────────────────────────────────────┘
 *       ...repeated for Pod 1, 2, ..., N-1
 * </pre>
 *
 * <h2>Parallelism</h2>
 * <ul>
 *   <li><b>Cross-pod</b>: Each K8s pod reads a distinct byte range of the S3 object
 *       using S3's native range-GET. {@code JOB_COMPLETION_INDEX} and {@code TOTAL_PODS} drive the split.
 *   <li><b>Within-pod (local chunking)</b>: A single reader thread produces chunks
 *       sequentially (optimal for disk I/O). Each chunk is dispatched via
 *       {@link ChunkTaskExecutorItemWriter} to a pool of writer threads.
 *       Each writer thread runs the {@link ChunkProcessor} which performs the JDBC
 *       batch insert in its own transaction (producer-consumer pattern).
 * </ul>
 *
 * <h2>Why local chunking over partitioning?</h2>
 * <ul>
 *   <li>Sequential disk reads avoid random-seek contention from multiple reader threads.
 *   <li>DB writes — the true bottleneck — are parallelised across worker threads.
 *   <li>Built-in backpressure: the producer pauses when the bounded queue is full.
 *   <li>Simpler configuration: no partitioner, no step-scoped beans, no grid-size tuning.
 * </ul>
 */
@Configuration
public class BatchConfig {

    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

    private static final String INSERT_SQL =
            "INSERT INTO dimensions (grid_id, csi_id, person_id, country_code, economic_code) VALUES (?, ?, ?, ?, ?)";

    @Value("${JOB_COMPLETION_INDEX:0}")
    private int podIndex;

    @Value("${TOTAL_PODS:1}")
    private int totalPods;

    // ── Job ───────────────────────────────────────────────────────────────────

    @Bean
    public Job dimensionLoadJob(JobRepository jobRepository, Step loadStep) {
        return new JobBuilder("dimensionLoadJob-pod" + podIndex, jobRepository)
                .start(loadStep)
                .build();
    }

    // ── Step with Local Chunking (Spring Batch 6 new feature) ─────────────────

    @Bean
    public Step loadStep(JobRepository jobRepository,
                         JdbcTransactionManager transactionManager,
                         AppProperties props,
                         ByteRangeFlatFileItemReader itemReader,
                         DimensionItemProcessor itemProcessor,
                         ChunkTaskExecutorItemWriter<DimensionRecord> localChunkWriter) {
        return new StepBuilder("loadStep", jobRepository)
                .<DimensionRecord, DimensionRecord>chunk(props.batch().chunkSize())
                .transactionManager(transactionManager)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(localChunkWriter)
                .build();
    }

    // ── Reader: single-threaded, sequential I/O for the pod's byte range ──────

    @Bean
    public ByteRangeFlatFileItemReader byteRangeReader(S3FileService s3FileService) {
        long fileSize = s3FileService.getFileSize();
        FileRange podRange = FileRange.forPod(fileSize, podIndex, totalPods);

        log.info("Pod {}/{} — S3 file size={} bytes, range=[{}, {}), isLast={}",
                podIndex, totalPods, fileSize,
                podRange.startByte(), podRange.endByte(), podRange.isLast());

        return new ByteRangeFlatFileItemReader(s3FileService, podRange);
    }

    // ── Local Chunking: ChunkTaskExecutorItemWriter ──────────────────────────
    //
    // This is the key Spring Batch 6 feature. Instead of partitioning the step
    // into N worker steps (each with its own reader), we use a SINGLE reader
    // and dispatch each chunk to a pool of writer threads.
    //
    // Think of it like "remote chunking, but with local threads" — the same
    // producer-consumer model without the network overhead.

    @Bean
    public ChunkTaskExecutorItemWriter<DimensionRecord> localChunkWriter(
            ChunkProcessor<DimensionRecord> chunkProcessor,
            AppProperties props) {

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(props.batch().threadPoolSize());
        taskExecutor.setMaxPoolSize(props.batch().threadPoolSize());
        taskExecutor.setThreadNamePrefix("chunk-writer-");
        taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        taskExecutor.setAwaitTerminationSeconds(600);
        taskExecutor.afterPropertiesSet();

        return new ChunkTaskExecutorItemWriter<>(chunkProcessor, taskExecutor);
    }

    // ── ChunkProcessor: each worker thread writes one chunk in its own tx ────

    @Bean
    public ChunkProcessor<DimensionRecord> chunkProcessor(DataSource dataSource,
                                                           TransactionTemplate transactionTemplate) {
        JdbcBatchItemWriter<DimensionRecord> itemWriter = new JdbcBatchItemWriterBuilder<DimensionRecord>()
                .dataSource(dataSource)
                .sql(INSERT_SQL)
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setString(1, item.gridId());
                    ps.setString(2, item.csiId());
                    ps.setString(3, item.personId());
                    ps.setString(4, item.countryCode());
                    ps.setString(5, item.economicCode());
                })
                .build();

        return (chunk, contribution) -> transactionTemplate.executeWithoutResult(status -> {
            try {
                itemWriter.write(chunk);
                contribution.incrementWriteCount(chunk.size());
                contribution.setExitStatus(ExitStatus.COMPLETED);
            } catch (Exception e) {
                status.setRollbackOnly();
                contribution.incrementWriteSkipCount(chunk.size());
                contribution.setExitStatus(ExitStatus.FAILED.addExitDescription(e.getMessage()));
            }
        });
    }

}
