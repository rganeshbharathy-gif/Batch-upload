package com.example.batchupload.config;

import com.example.batchupload.listener.PodProcessingListener;
import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.model.FileRange;
import com.example.batchupload.processor.DimensionItemProcessor;
import com.example.batchupload.reader.ByteRangeFlatFileItemReader;
import com.example.batchupload.repository.PodProcessingLogRepository;
import com.example.batchupload.service.S3FileService;
import org.springframework.batch.core.step.listener.StepExecutionListener;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import jakarta.persistence.EntityManagerFactory;
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

    private static final String MERGE_SQL = """
            MERGE INTO dimensions d \
            USING (SELECT ? AS person_id, ? AS grid_id, ? AS country_code, ? AS sector_code \
                   FROM DUAL) s \
            ON (d.person_id = s.person_id) \
            WHEN MATCHED THEN \
                UPDATE SET d.grid_id      = s.grid_id, \
                           d.country_code = s.country_code, \
                           d.sector_code  = s.sector_code, \
                           d.load_date    = TRUNC(SYSDATE) \
            WHEN NOT MATCHED THEN \
                INSERT (person_id, grid_id, country_code, sector_code, load_date) \
                VALUES (s.person_id, s.grid_id, s.country_code, s.sector_code, TRUNC(SYSDATE))\
            """;

    @Value("${batch.pod.index}")
    private int podIndex;

    @Value("${batch.pod.total}")
    private int totalPods;

    @Value("${batch.chunk-size}")
    private int chunkSize;

    @Value("${batch.thread-pool-size}")
    private int threadPoolSize;

    // ── Job ───────────────────────────────────────────────────────────────────

    @Bean
    public Job dimensionLoadJob(JobRepository jobRepository, Step loadStep) {
        return new JobBuilder("dimensionLoadJob-pod" + podIndex, jobRepository)
                .start(loadStep)
                .build();
    }

    // ── Step with Multi-Threaded Execution ────────────────────────────────────

    @Bean
    public Step loadStep(JobRepository jobRepository,
                         PlatformTransactionManager transactionManager,
                         ItemReader<DimensionRecord> itemReader,
                         DimensionItemProcessor itemProcessor,
                         JdbcBatchItemWriter<DimensionRecord> itemWriter,
                         AsyncTaskExecutor batchTaskExecutor,
                         StepExecutionListener podProcessingListener) {
        return new StepBuilder("loadStep", jobRepository)
                .<DimensionRecord, DimensionRecord>chunk(chunkSize)
                .transactionManager(transactionManager)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .listener(podProcessingListener)
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    // ── Reader: for the pod's byte range ──────────────────────────────────────

    @Bean
    @StepScope
    public ByteRangeFlatFileItemReader byteRangeReader(
            S3FileService s3FileService,
            @Value("#{jobParameters['s3.bucket']}") String bucket,
            @Value("#{jobParameters['s3.key']}") String s3Key) {
        long fileSize = s3FileService.getFileSize(bucket, s3Key);
        FileRange podRange = FileRange.forPod(fileSize, podIndex, totalPods);

        log.info("Pod {}/{} — bucket={} key={} fileSize={} bytes, range=[{}, {}), isLast={}",
                podIndex, totalPods, bucket, s3Key, fileSize,
                podRange.startByte(), podRange.endByte(), podRange.isLast());

        return new ByteRangeFlatFileItemReader(s3FileService, podRange, bucket, s3Key);
    }

    // ── Listener: logs pod processing metadata to DB ───────────────────────────

    @Bean
    @StepScope
    public PodProcessingListener podProcessingListener(
            PodProcessingLogRepository podProcessingLogRepository,
            S3FileService s3FileService,
            @Value("#{jobParameters['s3.bucket']}") String bucket,
            @Value("#{jobParameters['s3.key']}") String s3Key) {
        long fileSize = s3FileService.getFileSize(bucket, s3Key);
        FileRange podRange = FileRange.forPod(fileSize, podIndex, totalPods);

        return new PodProcessingListener(
                podProcessingLogRepository,
                podIndex, totalPods,
                bucket, s3Key,
                podRange.startByte(), podRange.endByte());
    }

    // ── Writer: JdbcBatchItemWriter ───────────────────────────────────────────

    @Bean
    public JdbcBatchItemWriter<DimensionRecord> itemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<DimensionRecord>()
                .dataSource(dataSource)
                .sql(MERGE_SQL)
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setString(1, item.personId());
                    ps.setString(2, item.gridId());
                    ps.setString(3, item.countryCode());
                    ps.setString(4, item.sectorCode());
                })
                .build();
    }

    // ── Transaction Managers ─────────────────────────────────────────────────

    /**
     * JDBC transaction manager used by Spring Batch steps.
     */
    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new org.springframework.jdbc.support.JdbcTransactionManager(dataSource);
    }

    /**
     * JPA transaction manager — marked {@code @Primary} so it is the default
     * for {@code @Transactional} across the application.
     * The Batch step receives its {@code JdbcTransactionManager} explicitly via method injection.
     */
    @Bean
    @org.springframework.context.annotation.Primary
    public org.springframework.orm.jpa.JpaTransactionManager jpaTransactionManager(EntityManagerFactory emf) {
        return new org.springframework.orm.jpa.JpaTransactionManager(emf);
    }

    // ── TaskExecutor for multi-threaded step ──────────────────────────────────

    @Bean
    public AsyncTaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(threadPoolSize);
        taskExecutor.setMaxPoolSize(threadPoolSize);
        taskExecutor.setThreadNamePrefix("batch-writer-");
        taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        taskExecutor.setAwaitTerminationSeconds(600);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

}
