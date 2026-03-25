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
        Integer podIndex,
        LocalDateTime createdAt) {

    /** Convenience constructor for inserts — id, createdAt are DB-generated. */
    public DimensionRecord(String gridId, String personId, String countryCode, String sectorCode, int podIndex) {
        this(null, gridId, personId, countryCode, sectorCode, podIndex, null);
    }
}
