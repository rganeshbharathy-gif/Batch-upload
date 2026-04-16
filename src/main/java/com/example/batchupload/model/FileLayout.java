package com.example.batchupload.model;

/**
 * Describes the on-disk layout of the daily input file:
 *
 * <pre>
 *   byte 0                                                       fileSize
 *   ├─ metadata line (line 1) ─┬─ header line (line 2) ─┬─ data rows ─┬─ footer line ─┤
 *                              │                        │             │               │
 *                              │                        dataStart    dataEnd
 * </pre>
 *
 * <p>{@code dataStart} is the byte offset of the first character <em>after</em> the
 * header row's terminating newline. {@code dataEnd} is the byte offset of the first
 * character of the footer row (i.e. the exclusive end of the data region).
 *
 * <p>{@code columnIndex} holds the zero-based position of each required field as
 * resolved from the header row.
 */
public record FileLayout(
        long dataStart,
        long dataEnd,
        ColumnIndices columnIndex) {

    /** Length of the pure data region in bytes. */
    public long dataLength() {
        return dataEnd - dataStart;
    }

    /**
     * Zero-based positions of the required columns in each pipe-delimited data row.
     *
     * @param gridId         index of {@code GRID_ID}
     * @param personId       index of {@code CSI_ID} (mapped to person_id)
     * @param countryCode    index of {@code CTY_OF_CTZN_CD}
     * @param ecoSectorCode  index of {@code ECON_SEC_CD}
     */
    public record ColumnIndices(
            int gridId,
            int personId,
            int countryCode,
            int ecoSectorCode) {

        /** Largest index that a data row must contain to be valid. */
        public int maxIndex() {
            return Math.max(Math.max(gridId, personId), Math.max(countryCode, ecoSectorCode));
        }
    }
}
