package com.example.batchupload.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Strongly-typed binding for all {@code batch.*} entries in application.yml.
 *
 * <p>Column indices are <strong>0-based</strong> positions of the target fields
 * inside the pipe-delimited source file (which may have 250+ columns).
 *
 * <p>Example:
 * <pre>
 * batch:
 *   file:
 *     path: /data/input.txt
 *     has-header: true
 *   partitions: 20
 *   chunk-size: 5000
 *   columns:
 *     csi-id: 0
 *     person-id: 1
 *     country-code: 10
 *     economic-code: 25
 * </pre>
 */
@ConfigurationProperties(prefix = "batch")
public record BatchProperties(
        FileConfig file,
        int partitions,
        int chunkSize,
        ColumnIndices columns
) {

    /**
     * Source file configuration.
     *
     * @param path      absolute path to the pipe-delimited text file
     * @param hasHeader {@code true} if the first line is a header row to be skipped
     */
    public record FileConfig(String path, boolean hasHeader) {}

    /**
     * Zero-based column indices of the four fields to extract.
     *
     * @param csiId        column index for CSI_ID
     * @param personId     column index for PERSON_ID
     * @param countryCode  column index for COUNTRY_CODE
     * @param economicCode column index for ECONOMIC_CODE
     */
    public record ColumnIndices(int csiId, int personId, int countryCode, int economicCode) {}
}
