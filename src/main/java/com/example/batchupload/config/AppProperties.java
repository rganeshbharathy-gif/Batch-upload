package com.example.batchupload.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Strongly-typed configuration bound from {@code application.yml}.
 * All values can be overridden via environment variables using Spring Boot's
 * relaxed binding (e.g. {@code APP_FILE_PATH}, {@code APP_COLUMNS_CSI_ID_INDEX}).
 */
@Data
@Component
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private File file = new File();
    private Columns columns = new Columns();
    private Batch batch = new Batch();

    @Data
    public static class File {
        /** Absolute path to the input pipe-delimited file (mounted PVC in K8s). */
        private String path = "/data/input.txt";
    }

    @Data
    public static class Columns {
        /** Zero-based column index for csi_id in the pipe-delimited file. */
        private int csiIdIndex = 0;

        /** Zero-based column index for person_id in the pipe-delimited file. */
        private int personIdIndex = 1;

        /** Zero-based column index for country_code in the pipe-delimited file. */
        private int countryCodeIndex = 2;

        /** Zero-based column index for economic_code in the pipe-delimited file. */
        private int economicCodeIndex = 3;
    }

    @Data
    public static class Batch {
        /**
         * Number of records processed per JDBC batch insert.
         * Optimal range for Oracle: 2 000 – 10 000.
         */
        private int chunkSize = 5000;

        /**
         * Number of parallel threads used within this pod.
         * Each thread reads its own sub-range of the pod's byte range.
         * Set to 1 to keep it single-threaded (simplest, safest).
         */
        private int threadPoolSize = 4;
    }
}
