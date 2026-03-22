package com.example.batchupload.model;

/**
 * Immutable value type representing the four columns extracted from each row
 * of the pipe-delimited source file and persisted to the DIMENSIONS table.
 *
 * <p>Java 25 records are used deliberately: zero-boilerplate, value semantics,
 * and naturally thread-safe – important when hundreds of virtual threads share
 * items in the chunk buffer.
 */
public record DimensionRecord(
        String csiId,
        String personId,
        String countryCode,
        String economicCode
) {}
