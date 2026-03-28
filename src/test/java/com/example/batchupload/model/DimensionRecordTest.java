package com.example.batchupload.model;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class DimensionRecordTest {

    @Test
    void convenienceConstructor_setsNullCreatedAt() {
        DimensionRecord record = new DimensionRecord("P1", "G1", "US", "TECH");
        assertThat(record.createdAt()).isNull();
        assertThat(record.personId()).isEqualTo("P1");
        assertThat(record.gridId()).isEqualTo("G1");
        assertThat(record.countryCode()).isEqualTo("US");
        assertThat(record.sectorCode()).isEqualTo("TECH");
    }

    @Test
    void fullConstructor_preservesAllFields() {
        LocalDateTime now = LocalDateTime.now();
        DimensionRecord record = new DimensionRecord("P1", "G1", "US", "TECH", now);
        assertThat(record.personId()).isEqualTo("P1");
        assertThat(record.gridId()).isEqualTo("G1");
        assertThat(record.countryCode()).isEqualTo("US");
        assertThat(record.sectorCode()).isEqualTo("TECH");
        assertThat(record.createdAt()).isEqualTo(now);
    }
}
