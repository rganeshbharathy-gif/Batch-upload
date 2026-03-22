package com.example.batchupload.config;

import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.model.FileRange;
import com.example.batchupload.processor.DimensionItemProcessor;
import com.example.batchupload.reader.ByteRangeFlatFileItemReader;
import com.example.batchupload.service.S3FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

/**
 * Spring Batch 6 configuration for the Dimension Load job using a <b>multi-threaded step</b>.
 *
 * <h2>Architecture (Multi-Threaded Step — Spring Batch 6)</h2>
 * <pre>
 *  Kubernetes Indexed Job
 *  ┌─────────────────────────────────────────────────────────┐
 *  │  Pod 0  (JOB_COMPLETION_INDEX=0)                        │
 *  │  ┌───────────────────────────────────────────────────┐  │
 *  │  │ dimensionLoadJob                                  │  │
 *  │  │   loadStep (multi-threaded)                       │  │
 *  │  │                                                   │  │
 *  │  │   thread-0: [Reader → Processor → Writer] (tx)    │  │
 *  │  │   thread-1: [Reader → Processor → Writer] (tx)    │  │
 *  │  │   thread-2: [Reader → Processor → Writer] (tx)    │  │
 *  │  │   thread-3: [Reader → Processor → Writer] (tx)    │  │
 *  │  │     (parallel chunks, each in own transaction)    │  │
 *  │  └───────────────────────────────────────────────────┘  │
 *  └─────────────────────────────────────────────────────────┘
 *       ...repeated for Pod 1, 2, ..., N-1
 * </pre>
 *
 * <h2>Parallelism</h2>
 * <ul>
 *   <li><b>Cross-pod</b>: Each K8s pod reads a distinct byte range of the S3 object
 *       using S3's native range-GET. {@code JOB_COMPLETION_INDEX} and {@code TOTAL_PODS} drive the split.
 *   <li><b>Within-pod</b>: A {@link TaskExecutor} on the step enables multiple threads
 *       to each read, process, and write chunks concurrently. The reader must be thread-safe.
 * </ul>
 */
@Configuration
public class BatchConfig {

    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

    private static final String INSERT_SQL =
            "INSERT INTO dimensions (grid_id, person_id, country_code, sector_code) VALUES (?, ?, ?, ?)";

    private static final int POD_INDEX = 0;
    private static final int TOTAL_PODS = 1;

    private static final int CHUNK_SIZE = 5000;
    private static final int THREAD_POOL_SIZE = 4;

    // ── Job ───────────────────────────────────────────────────────────────────

    @Bean
    public Job dimensionLoadJob(JobRepository jobRepository, Step loadStep) {
        return new JobBuilder("dimensionLoadJob-pod" + POD_INDEX, jobRepository)
                .start(loadStep)
                .build();
    }

    // ── Step with Multi-Threaded Execution ────────────────────────────────────

    @Bean
    public Step loadStep(JobRepository jobRepository,
                         JdbcTransactionManager transactionManager,
                         ByteRangeFlatFileItemReader itemReader,
                         DimensionItemProcessor itemProcessor,
                         JdbcBatchItemWriter<DimensionRecord> itemWriter,
                         AsyncTaskExecutor batchTaskExecutor) {
        return new StepBuilder("loadStep", jobRepository)
                .<DimensionRecord, DimensionRecord>chunk(CHUNK_SIZE)
                .transactionManager(transactionManager)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    // ── Reader: for the pod's byte range ──────────────────────────────────────

    @Bean
    public ByteRangeFlatFileItemReader byteRangeReader(S3FileService s3FileService) {
        long fileSize = s3FileService.getFileSize();
        FileRange podRange = FileRange.forPod(fileSize, POD_INDEX, TOTAL_PODS);

        log.info("Pod {}/{} — S3 file size={} bytes, range=[{}, {}), isLast={}",
                POD_INDEX, TOTAL_PODS, fileSize,
                podRange.startByte(), podRange.endByte(), podRange.isLast());

        return new ByteRangeFlatFileItemReader(s3FileService, podRange);
    }

    // ── Writer: JdbcBatchItemWriter ───────────────────────────────────────────

    @Bean
    public JdbcBatchItemWriter<DimensionRecord> itemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<DimensionRecord>()
                .dataSource(dataSource)
                .sql(INSERT_SQL)
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setString(1, item.gridId());
                    ps.setString(2, item.personId());
                    ps.setString(3, item.countryCode());
                    ps.setString(4, item.sectorCode());
                })
                .build();
    }

    // ── Transaction Manager ──────────────────────────────────────────────────

    @Bean
    public JdbcTransactionManager transactionManager(DataSource dataSource) {
        return new JdbcTransactionManager(dataSource);
    }

    // ── TaskExecutor for multi-threaded step ──────────────────────────────────

    @Bean
    public AsyncTaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(THREAD_POOL_SIZE);
        taskExecutor.setMaxPoolSize(THREAD_POOL_SIZE);
        taskExecutor.setThreadNamePrefix("batch-writer-");
        taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        taskExecutor.setAwaitTerminationSeconds(600);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

}
