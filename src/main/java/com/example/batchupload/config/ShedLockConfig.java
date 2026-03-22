package com.example.batchupload.config;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;

/**
 * Registers ShedLock with the JDBC provider backed by the same Oracle
 * {@link DataSource} already used by the batch job.
 *
 * <h2>Why ShedLock is needed here</h2>
 * When this application runs in a multi-instance deployment (e.g. two pods in
 * Kubernetes both triggered by a cron job at the same time), both instances
 * would attempt to load the same 20 GB file. ShedLock ensures that only the
 * <em>first</em> instance to acquire the lock proceeds; all others skip the
 * run gracefully.
 *
 * <h2>Lock storage</h2>
 * ShedLock stores a single row per named lock in the {@code SHEDLOCK} table.
 * The row contains three timestamps:
 * <ul>
 *   <li>{@code lock_until} – the lock is considered stale after this instant;
 *       another node may claim it.</li>
 *   <li>{@code locked_at}  – when the lock was first acquired.</li>
 *   <li>{@code locked_by}  – hostname + thread name of the owner.</li>
 * </ul>
 *
 * <h2>{@code @EnableScheduling} note</h2>
 * {@code @EnableSchedulerLock} requires {@code @EnableScheduling} to be
 * present somewhere in the context. We declare it here so there is a single
 * canonical location. The batch job itself is <em>not</em> triggered by
 * {@code @Scheduled} – we use programmatic locking via
 * {@link net.javacrumbs.shedlock.core.LockProvider} directly in
 * {@link com.example.batchupload.runner.JobLauncherRunner}.
 */
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "PT4H") // 4-hour safety ceiling
public class ShedLockConfig {

    /**
     * JDBC-backed lock provider.
     *
     * <p>{@link JdbcTemplateLockProvider.Configuration#usingDbTime()} tells
     * ShedLock to use {@code SYSTIMESTAMP} (Oracle DB time) for all timestamp
     * comparisons instead of the application-server clock. This is critical
     * in multi-node deployments where server clocks may skew by seconds or
     * minutes, which could cause the lock to appear expired on one node while
     * still valid on another.
     */
    @Bean
    public LockProvider lockProvider(DataSource dataSource) {
        return new JdbcTemplateLockProvider(
                JdbcTemplateLockProvider.Configuration.builder()
                        .withJdbcTemplate(new JdbcTemplate(dataSource))
                        .usingDbTime()           // use Oracle SYSTIMESTAMP, not JVM clock
                        .withTableName("SHEDLOCK")
                        .build()
        );
    }
}
