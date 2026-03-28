package com.example.batchupload.model;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class DimensionRecordTest {

    @Test
    void convenienceConstructor_setsNullIdAndCreatedAt() {
        DimensionRecord record = new DimensionRecord("G1", "P1", "US", "TECH", 2);
        assertThat(record.id()).isNull();
        assertThat(record.createdAt()).isNull();
        assertThat(record.gridId()).isEqualTo("G1");
        assertThat(record.personId()).isEqualTo("P1");
        assertThat(record.countryCode()).isEqualTo("US");
        assertThat(record.sectorCode()).isEqualTo("TECH");
        assertThat(record.podIndex()).isEqualTo(2);
    }

    @Test
    void fullConstructor_preservesAllFields() {
        LocalDateTime now = LocalDateTime.now();
        DimensionRecord record = new DimensionRecord(42L, "G1", "P1", "US", "TECH", 3, now);
        assertThat(record.id()).isEqualTo(42L);
        assertThat(record.gridId()).isEqualTo("G1");
        assertThat(record.personId()).isEqualTo("P1");
        assertThat(record.countryCode()).isEqualTo("US");
        assertThat(record.sectorCode()).isEqualTo("TECH");
        assertThat(record.podIndex()).isEqualTo(3);
        assertThat(record.createdAt()).isEqualTo(now);
    }
}
