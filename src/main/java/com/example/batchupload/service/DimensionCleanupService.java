package com.example.batchupload.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Removes stale rows from DIMENSIONS after a daily MERGE load.
 *
 * <p>After all pods complete their MERGE (which stamps {@code LOAD_DATE = TRUNC(SYSDATE)}
 * on every row they touch), any row whose {@code LOAD_DATE} is older than today was
 * absent from the daily file and should be deleted.
 */
@Service
public class DimensionCleanupService {

    private static final Logger log = LoggerFactory.getLogger(DimensionCleanupService.class);

    private final JdbcTemplate jdbcTemplate;

    public DimensionCleanupService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Deletes all DIMENSIONS rows whose LOAD_DATE is before today.
     *
     * @return number of stale rows deleted
     */
    public int deleteStaleRows() {
        int deleted = jdbcTemplate.update(
                "DELETE FROM DIMENSIONS WHERE LOAD_DATE < TRUNC(SYSDATE)");
        log.info("Stale row cleanup complete — deleted {} rows from DIMENSIONS", deleted);
        return deleted;
    }
}
