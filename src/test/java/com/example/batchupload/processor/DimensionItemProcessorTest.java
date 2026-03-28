package com.example.batchupload.processor;

import com.example.batchupload.model.DimensionRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DimensionItemProcessorTest {

    private final DimensionItemProcessor processor = new DimensionItemProcessor();

    @Test
    void process_validRecord_returnsRecord() throws Exception {
        DimensionRecord input = new DimensionRecord("P1", "G1", "US", "TECH");
        DimensionRecord result = processor.process(input);

        assertThat(result).isNotNull();
        assertThat(result.personId()).isEqualTo("P1");
        assertThat(result.gridId()).isEqualTo("G1");
        assertThat(result.countryCode()).isEqualTo("US");
        assertThat(result.sectorCode()).isEqualTo("TECH");
    }

    @Test
    void process_blankPersonId_returnsNull() throws Exception {
        DimensionRecord input = new DimensionRecord("", "G1", "US", "TECH");
        assertThat(processor.process(input)).isNull();
    }

    @Test
    void process_nullPersonId_returnsNull() throws Exception {
        DimensionRecord input = new DimensionRecord(null, "G1", "US", "TECH");
        assertThat(processor.process(input)).isNull();
    }

    @Test
    void process_blankGridId_returnsRecord() throws Exception {
        DimensionRecord input = new DimensionRecord("P1", "", "US", "TECH");
        assertThat(processor.process(input)).isNotNull();
    }

    @Test
    void process_truncatesLongFields() throws Exception {
        String longPersonId = "P".repeat(150);
        DimensionRecord input = new DimensionRecord(longPersonId, "G1", "US", "TECH");
        DimensionRecord result = processor.process(input);

        assertThat(result).isNotNull();
        assertThat(result.personId()).hasSize(100);
    }

    @Test
    void process_nullCountryCodeStaysNull() throws Exception {
        DimensionRecord input = new DimensionRecord("P1", "G1", null, "TECH");
        DimensionRecord result = processor.process(input);

        assertThat(result).isNotNull();
        assertThat(result.countryCode()).isNull();
    }
}
