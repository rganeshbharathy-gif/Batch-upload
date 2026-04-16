package com.example.batchupload;

import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.model.FileLayout;
import com.example.batchupload.model.FileLayout.ColumnIndices;
import com.example.batchupload.model.FileRange;
import com.example.batchupload.reader.ByteRangeFlatFileItemReader;
import com.example.batchupload.reader.FileLayoutScanner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the layout scanner, byte-range reader, and partition arithmetic.
 * No Spring context — plain JUnit against a handful of tiny fixtures.
 */
class BatchUploadApplicationTests {

    @TempDir
    Path tempDir;

    private static final Instant FIXED_LOAD_TIME = Instant.parse("2026-04-16T00:00:00Z");

    private Path writeStandardFile() throws IOException {
        // line 1: vendor metadata, line 2: header, lines 3-14: data, line 15: footer
        StringBuilder sb = new StringBuilder();
        sb.append("FILE=DAILY|DATE=2026-04-16|VENDOR=ACME\n");
        sb.append("IGNORED|GRID_ID|FILLER|CSI_ID|CTY_OF_CTZN_CD|ECON_SEC_CD|TAIL\n");
        for (int i = 0; i < 12; i++) {
            sb.append("x|G").append(i).append("|y|P").append(i)
              .append("|CC").append(i).append("|").append(100 + i).append("|z\n");
        }
        sb.append("FOOTER|ROWCOUNT=12\n");

        Path file = tempDir.resolve("input.txt");
        Files.writeString(file, sb.toString());
        return file;
    }

    @Test
    void scannerResolvesColumnsAndDataBoundaries() throws Exception {
        Path file = writeStandardFile();
        FileLayout layout = new FileLayoutScanner(
                file, "GRID_ID", "CSI_ID", "CTY_OF_CTZN_CD", "ECON_SEC_CD").scan();

        assertThat(layout.columnIndex()).isEqualTo(new ColumnIndices(1, 3, 4, 5));
        assertThat(layout.dataStart()).isGreaterThan(0);
        assertThat(layout.dataEnd()).isLessThan(Files.size(file));

        // The data region must correspond to exactly the 12 data lines.
        byte[] all = Files.readAllBytes(file);
        String dataRegion = new String(all, (int) layout.dataStart(),
                (int) (layout.dataEnd() - layout.dataStart()), StandardCharsets.UTF_8);
        assertThat(dataRegion.split("\n")).hasSize(12);
        assertThat(dataRegion).doesNotContain("FOOTER").doesNotContain("FILE=DAILY");
    }

    @Test
    void readerCoversAllDataLinesAcrossPartitionsWithNoOverlap() throws Exception {
        Path file = writeStandardFile();
        FileLayout layout = new FileLayoutScanner(
                file, "GRID_ID", "CSI_ID", "CTY_OF_CTZN_CD", "ECON_SEC_CD").scan();

        int totalPods = 3;
        List<DimensionRecord> all = new ArrayList<>();
        for (int pod = 0; pod < totalPods; pod++) {
            FileRange range = FileRange.forPod(layout.dataStart(), layout.dataEnd(), pod, totalPods);
            ByteRangeFlatFileItemReader reader = new ByteRangeFlatFileItemReader(
                    file, range, layout.dataStart(), layout.columnIndex(), FIXED_LOAD_TIME);
            reader.open(new ExecutionContext());
            DimensionRecord r;
            while ((r = reader.read()) != null) all.add(r);
            reader.close();
        }

        assertThat(all).hasSize(12);
        for (int i = 0; i < 12; i++) {
            int idx = i;
            assertThat(all).anyMatch(r ->
                    ("P" + idx).equals(r.personId())
                            && ("G" + idx).equals(r.gridId())
                            && Long.valueOf(100L + idx).equals(r.ecoSectorCode())
                            && FIXED_LOAD_TIME.equals(r.loadedAt()));
        }
    }

    @Test
    void readerSkipsMalformedAndBlankLines() throws Exception {
        // Hand-crafted: one malformed (too few fields) and one blank line mixed in.
        String content = """
                META|LINE
                A|GRID_ID|B|CSI_ID|CTY_OF_CTZN_CD|ECON_SEC_CD
                a|G0|b|P0|CC0|100

                short|line
                a|G1|b|P1|CC1|101
                FOOTER|ROWS=2
                """;
        Path file = tempDir.resolve("dirty.txt");
        Files.writeString(file, content);

        FileLayout layout = new FileLayoutScanner(
                file, "GRID_ID", "CSI_ID", "CTY_OF_CTZN_CD", "ECON_SEC_CD").scan();
        FileRange range = FileRange.forPod(layout.dataStart(), layout.dataEnd(), 0, 1);

        ByteRangeFlatFileItemReader reader = new ByteRangeFlatFileItemReader(
                file, range, layout.dataStart(), layout.columnIndex(), FIXED_LOAD_TIME);
        reader.open(new ExecutionContext());

        List<DimensionRecord> out = new ArrayList<>();
        DimensionRecord r;
        while ((r = reader.read()) != null) out.add(r);
        reader.close();

        assertThat(out).hasSize(2);
        assertThat(out).extracting(DimensionRecord::personId).containsExactly("P0", "P1");
    }

    @Test
    void readerSkipsRowsWithNonNumericEcoSectorCode() throws Exception {
        String content = """
                META
                GRID_ID|CSI_ID|CTY_OF_CTZN_CD|ECON_SEC_CD
                G0|P0|CC0|100
                G1|P1|CC1|NOT_A_NUMBER
                G2|P2|CC2|102
                FOOTER|ROWS=2
                """;
        Path file = tempDir.resolve("bad-eco.txt");
        Files.writeString(file, content);

        FileLayout layout = new FileLayoutScanner(
                file, "GRID_ID", "CSI_ID", "CTY_OF_CTZN_CD", "ECON_SEC_CD").scan();
        FileRange range = FileRange.forPod(layout.dataStart(), layout.dataEnd(), 0, 1);
        ByteRangeFlatFileItemReader reader = new ByteRangeFlatFileItemReader(
                file, range, layout.dataStart(), layout.columnIndex(), FIXED_LOAD_TIME);
        reader.open(new ExecutionContext());

        List<DimensionRecord> out = new ArrayList<>();
        DimensionRecord r;
        while ((r = reader.read()) != null) out.add(r);
        reader.close();

        assertThat(out).extracting(DimensionRecord::personId).containsExactly("P0", "P2");
        assertThat(out).extracting(DimensionRecord::ecoSectorCode).containsExactly(100L, 102L);
    }

    @Test
    void fileRangeForPodCoversEntireDataRegion() {
        long dataStart = 1024L;
        long dataEnd = 20_000_000_000L;   // ~20 GB data region
        int totalPods = 5;
        long covered = 0;
        for (int i = 0; i < totalPods; i++) {
            FileRange range = FileRange.forPod(dataStart, dataEnd, i, totalPods);
            covered += range.length();
        }
        assertThat(covered).isEqualTo(dataEnd - dataStart);
    }
}
