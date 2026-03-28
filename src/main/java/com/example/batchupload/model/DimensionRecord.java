package com.example.batchupload.model;

import java.time.LocalDateTime;

/**
 * Represents a single row in the DIMENSIONS table.
 * {@code personId} is the natural primary key.
 * {@code createdAt} defaults to CURRENT_TIMESTAMP in the DB.
 */
public record DimensionRecord(
        String personId,
        String gridId,
        String countryCode,
        String sectorCode,
        LocalDateTime createdAt) {

    /** Convenience constructor for inserts — createdAt is DB-generated. */
    public DimensionRecord(String personId, String gridId, String countryCode, String sectorCode) {
        this(personId, gridId, countryCode, sectorCode, null);
    }
}
