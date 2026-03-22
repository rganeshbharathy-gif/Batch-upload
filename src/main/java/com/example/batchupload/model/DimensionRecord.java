package com.example.batchupload.model;

import java.time.LocalDateTime;

/**
 * Represents a single row in the DIMENSIONS table.
 * {@code id} is auto-generated (IDENTITY) and {@code createdAt} defaults to
 * CURRENT_TIMESTAMP — both are {@code null} when constructing records for insert.
 */
public record DimensionRecord(
        Long id,
        String csiId,
        String personId,
        String countryCode,
        String economicCode,
        LocalDateTime createdAt) {

    /** Convenience constructor for inserts — id and createdAt are DB-generated. */
    public DimensionRecord(String csiId, String personId, String countryCode, String economicCode) {
        this(null, csiId, personId, countryCode, economicCode, null);
    }
}
