package com.example.batchupload.processor;

import com.example.batchupload.model.DimensionRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DimensionItemProcessorTest {

    private final DimensionItemProcessor processor = new DimensionItemProcessor();

    @Test
    void process_validRecord_returnsRecord() throws Exception {
        DimensionRecord input = new DimensionRecord("G1", "P1", "US", "TECH");
        DimensionRecord result = processor.process(input);

        assertThat(result).isNotNull();
        assertThat(result.gridId()).isEqualTo("G1");
        assertThat(result.personId()).isEqualTo("P1");
        assertThat(result.countryCode()).isEqualTo("US");
        assertThat(result.sectorCode()).isEqualTo("TECH");
    }

    @Test
    void process_bothBlank_returnsNull() throws Exception {
        DimensionRecord input = new DimensionRecord("", "", "US", "TECH");
        assertThat(processor.process(input)).isNull();
    }

    @Test
    void process_onlyOneBlank_returnsRecord() throws Exception {
        DimensionRecord input = new DimensionRecord("", "P1", "US", "TECH");
        assertThat(processor.process(input)).isNotNull();
    }

    @Test
    void process_truncatesLongFields() throws Exception {
        String longGridId = "G".repeat(150);
        DimensionRecord input = new DimensionRecord(longGridId, "P1", "US", "TECH");
        DimensionRecord result = processor.process(input);

        assertThat(result).isNotNull();
        assertThat(result.gridId()).hasSize(100);
    }

    @Test
    void process_nullFieldsHandled() throws Exception {
        DimensionRecord input = new DimensionRecord(null, "P1", null, "TECH");
        DimensionRecord result = processor.process(input);

        assertThat(result).isNotNull();
        assertThat(result.countryCode()).isNull();
    }
}
