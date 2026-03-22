package com.example.batchupload.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Strongly-typed configuration bound from {@code application.yml}.
 * All values can be overridden via environment variables using Spring Boot's
 * relaxed binding (e.g. {@code APP_FILE_PATH}, {@code APP_COLUMNS_CSI_ID_INDEX}).
 */
@Component
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private S3 s3 = new S3();
    private Columns columns = new Columns();
    private Batch batch = new Batch();

    public S3 getS3() { return s3; }
    public void setS3(S3 s3) { this.s3 = s3; }
    public Columns getColumns() { return columns; }
    public void setColumns(Columns columns) { this.columns = columns; }
    public Batch getBatch() { return batch; }
    public void setBatch(Batch batch) { this.batch = batch; }

    public static class S3 {
        private String bucket;
        private String key;
        private String region = "us-east-1";

        public String getBucket() { return bucket; }
        public void setBucket(String bucket) { this.bucket = bucket; }
        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
    }

    public static class Columns {
        private int csiIdIndex = 0;
        private int personIdIndex = 1;
        private int countryCodeIndex = 2;
        private int economicCodeIndex = 3;

        public int getCsiIdIndex() { return csiIdIndex; }
        public void setCsiIdIndex(int csiIdIndex) { this.csiIdIndex = csiIdIndex; }
        public int getPersonIdIndex() { return personIdIndex; }
        public void setPersonIdIndex(int personIdIndex) { this.personIdIndex = personIdIndex; }
        public int getCountryCodeIndex() { return countryCodeIndex; }
        public void setCountryCodeIndex(int countryCodeIndex) { this.countryCodeIndex = countryCodeIndex; }
        public int getEconomicCodeIndex() { return economicCodeIndex; }
        public void setEconomicCodeIndex(int economicCodeIndex) { this.economicCodeIndex = economicCodeIndex; }
    }

    public static class Batch {
        /** Rows per JDBC batch insert. Optimal range for Oracle: 2000–10000. */
        private int chunkSize = 5000;

        /** Number of parallel writer threads for local chunking within this pod. */
        private int threadPoolSize = 4;

        public int getChunkSize() { return chunkSize; }
        public void setChunkSize(int chunkSize) { this.chunkSize = chunkSize; }
        public int getThreadPoolSize() { return threadPoolSize; }
        public void setThreadPoolSize(int threadPoolSize) { this.threadPoolSize = threadPoolSize; }
    }
}
