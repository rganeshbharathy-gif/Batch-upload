package com.example.batchupload.model;

import java.time.Instant;

/**
 * Row to be merged into the DIMENSIONS table.
 *
 * <p>Column mapping (file → record → DB):
 * <pre>
 *   GRID_ID         → gridId        → grid_id
 *   CSI_ID          → personId      → person_id       (primary key — merge target)
 *   CTY_OF_CTZN_CD  → countryCode   → country_code
 *   ECON_SEC_CD     → ecoSectorCode → eco_sector_code
 *                     loadedAt      → loaded_at       (job execution timestamp)
 * </pre>
 */
public record DimensionRecord(
        String gridId,
        String personId,
        String countryCode,
        Long ecoSectorCode,
        Instant loadedAt) {
}
