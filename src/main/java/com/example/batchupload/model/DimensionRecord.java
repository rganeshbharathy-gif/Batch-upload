package com.example.batchupload.model;

import java.time.LocalDateTime;

/**
 * Represents a single row in the DIMENSIONS table.
 * {@code id} is auto-generated (IDENTITY) and {@code createdAt} defaults to
 * CURRENT_TIMESTAMP — both are {@code null} when constructing records for insert.
 */
public record DimensionRecord(
        Long id,
        String gridId,
        String personId,
        String countryCode,
        String sectorCode,
        LocalDateTime createdAt) {

    /** Convenience constructor for inserts — id and createdAt are DB-generated. */
    public DimensionRecord(String gridId, String personId, String countryCode, String sectorCode) {
        this(null, gridId, personId, countryCode, sectorCode, null);
    }
}
