package com.example.batchupload.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Strongly-typed configuration bound from {@code application.yml}.
 * All values can be overridden via environment variables using Spring Boot's
 * relaxed binding (e.g. {@code APP_FILE_PATH}, {@code APP_COLUMNS_GRID_ID}).
 */
@ConfigurationProperties(prefix = "app")
public record AppProperties(File file, Columns columns, Batch batch) {

    public AppProperties {
        if (file == null)    file    = new File("/data/input.txt");
        if (columns == null) columns = Columns.defaults();
        if (batch == null)   batch   = Batch.defaults();
    }

    /** @param path Absolute path to the input pipe-delimited file (mounted PVC in K8s). */
    public record File(String path) {
    }

    /**
     * Header-row token (case-insensitive) that identifies each required column
     * in the input file. Defaults match the vendor specification.
     */
    public record Columns(
            String gridId,
            String personId,
            String countryCode,
            String ecoSectorCode) {

        public static Columns defaults() {
            return new Columns("GRID_ID", "CSI_ID", "CTY_OF_CTZN_CD", "ECON_SEC_CD");
        }
    }

    /**
     * @param chunkSize      Rows per JDBC batch merge. Oracle sweet-spot: 2 000–10 000.
     * @param threadPoolSize Worker threads within this pod. Total concurrency
     *                       across the cluster = {@code TOTAL_PODS × threadPoolSize}.
     * @param skipLimit      Max unrecoverable parse errors tolerated before the step fails.
     */
    public record Batch(int chunkSize, int threadPoolSize, int skipLimit) {

        public static Batch defaults() {
            return new Batch(5000, 4, 10_000);
        }
    }
}
