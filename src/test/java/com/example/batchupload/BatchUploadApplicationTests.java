package com.example.batchupload;

import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.model.FileRange;
import com.example.batchupload.reader.ByteRangeFlatFileItemReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.item.ExecutionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the byte-range reader and partition calculation.
 * Uses a tiny in-memory file — no Spring context needed.
 */
class BatchUploadApplicationTests {

    @TempDir
    Path tempDir;

    /**
     * Write a small pipe-delimited file, split it into 3 byte-ranges, read each
     * range and verify that every line is read exactly once.
     */
    @Test
    void readerShouldCoverAllLinesWithNoOverlap() throws Exception {
        // 12 lines; each line has 5 pipe-separated fields
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            lines.add("csi" + i + "|person" + i + "|CC" + i + "|EC" + i + "|extra" + i);
        }
        Path file = tempDir.resolve("test-input.txt");
        Files.write(file, lines);

        long fileSize = Files.size(file);
        int totalPods = 3;

        List<DimensionRecord> allRecords = new ArrayList<>();
        for (int podIdx = 0; podIdx < totalPods; podIdx++) {
            FileRange range = FileRange.forPod(fileSize, podIdx, totalPods);
            ByteRangeFlatFileItemReader reader = new ByteRangeFlatFileItemReader(
                    file, range, 0, 1, 2, 3);

            reader.open(new ExecutionContext());
            DimensionRecord record;
            while ((record = reader.read()) != null) {
                allRecords.add(record);
            }
            reader.close();
        }

        // All 12 lines should be read exactly once
        assertThat(allRecords).hasSize(12);
        for (int i = 0; i < 12; i++) {
            int finalI = i;
            assertThat(allRecords).anyMatch(r -> r.getCsiId().equals("csi" + finalI));
        }
    }

    @Test
    void fileRangeForPodShouldCoverEntireFile() {
        long fileSize = 20_000_000_000L; // 20 GB
        int totalPods = 5;
        long coveredBytes = 0;
        for (int i = 0; i < totalPods; i++) {
            FileRange range = FileRange.forPod(fileSize, i, totalPods);
            coveredBytes += range.length();
        }
        // Last pod picks up any remainder from integer division
        assertThat(coveredBytes).isEqualTo(fileSize);
    }

    @Test
    void readerShouldSkipBlankLines() throws Exception {
        Path file = tempDir.resolve("blanks.txt");
        Files.writeString(file, "a|b|c|d|e\n\n\nc|d|e|f|g\n");

        FileRange range = FileRange.forPod(Files.size(file), 0, 1);
        ByteRangeFlatFileItemReader reader = new ByteRangeFlatFileItemReader(
                file, range, 0, 1, 2, 3);
        reader.open(new ExecutionContext());

        List<DimensionRecord> records = new ArrayList<>();
        DimensionRecord r;
        while ((r = reader.read()) != null) records.add(r);
        reader.close();

        assertThat(records).hasSize(2);
    }

    @Test
    void readerShouldSkipMalformedLines() throws Exception {
        Path file = tempDir.resolve("malformed.txt");
        // Line 2 has only 2 fields, insufficient for index 3
        Files.writeString(file, "a|b|c|d|e\nonly|two\nc|d|e|f|g\n");

        FileRange range = FileRange.forPod(Files.size(file), 0, 1);
        ByteRangeFlatFileItemReader reader = new ByteRangeFlatFileItemReader(
                file, range, 0, 1, 2, 3);
        reader.open(new ExecutionContext());

        List<DimensionRecord> records = new ArrayList<>();
        DimensionRecord r;
        while ((r = reader.read()) != null) records.add(r);
        reader.close();

        assertThat(records).hasSize(2);
    }
}
