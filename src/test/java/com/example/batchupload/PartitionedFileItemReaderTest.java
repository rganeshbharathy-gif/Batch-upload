package com.example.batchupload;

import com.example.batchupload.config.BatchProperties.ColumnIndices;
import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.reader.PartitionedFileItemReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.item.ExecutionContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PartitionedFileItemReader}.
 * Uses a small temp file – no Spring context, no Oracle needed.
 */
class PartitionedFileItemReaderTest {

    @TempDir
    Path tmp;

    /**
     * Column layout used by all tests:
     * col 0 = csi_id, col 1 = person_id, col 2 = country_code, col 3 = economic_code
     */
    private static final ColumnIndices COLS = new ColumnIndices(0, 1, 2, 3);

    @Test
    void readsAllRecordsInRange() throws Exception {
        // 3 data rows, no header
        String content = "CSI1|PER1|US|EC1\nCSI2|PER2|GB|EC2\nCSI3|PER3|DE|EC3\n";
        Path file = writeFile("data.txt", content);
        long fileLen = Files.size(file);

        PartitionedFileItemReader reader =
                new PartitionedFileItemReader(file.toString(), 0, fileLen, COLS);

        List<DimensionRecord> records = readAll(reader);

        assertThat(records).hasSize(3);
        assertThat(records.get(0)).isEqualTo(new DimensionRecord("CSI1", "PER1", "US", "EC1"));
        assertThat(records.get(2)).isEqualTo(new DimensionRecord("CSI3", "PER3", "DE", "EC3"));
    }

    @Test
    void readsOnlyAssignedByteRange() throws Exception {
        // Two lines; we assign only the first line to the partition.
        String line1 = "A|B|C|D\n";
        String line2 = "E|F|G|H\n";
        Path file = writeFile("two.txt", line1 + line2);

        long endOfLine1 = line1.getBytes(StandardCharsets.UTF_8).length;

        PartitionedFileItemReader reader =
                new PartitionedFileItemReader(file.toString(), 0, endOfLine1, COLS);

        List<DimensionRecord> records = readAll(reader);

        assertThat(records).hasSize(1);
        assertThat(records.get(0).csiId()).isEqualTo("A");
    }

    @Test
    void handlesMoreThanFourColumns() throws Exception {
        // 10 columns; csi_id at 0, person_id at 1, country_code at 2, economic_code at 3
        String content = "C0|C1|C2|C3|C4|C5|C6|C7|C8|C9\n";
        Path file = writeFile("wide.txt", content);

        PartitionedFileItemReader reader =
                new PartitionedFileItemReader(file.toString(), 0, Files.size(file), COLS);

        List<DimensionRecord> records = readAll(reader);

        assertThat(records).hasSize(1);
        assertThat(records.get(0))
                .isEqualTo(new DimensionRecord("C0", "C1", "C2", "C3"));
    }

    @Test
    void skipsBlankLines() throws Exception {
        String content = "A|B|C|D\n\n   \nE|F|G|H\n";
        Path file = writeFile("blanks.txt", content);

        PartitionedFileItemReader reader =
                new PartitionedFileItemReader(file.toString(), 0, Files.size(file), COLS);

        List<DimensionRecord> records = readAll(reader);

        assertThat(records).hasSize(2);
    }

    @Test
    void restartSkipsPreviouslyReadLines() throws Exception {
        String content = "A|B|C|D\nE|F|G|H\nI|J|K|L\n";
        Path file = writeFile("restart.txt", content);

        // Simulate a restart after 1 line was already processed
        ExecutionContext ctx = new ExecutionContext();
        ctx.putLong("items.read", 1L);

        PartitionedFileItemReader reader =
                new PartitionedFileItemReader(file.toString(), 0, Files.size(file), COLS);

        reader.open(ctx);
        List<DimensionRecord> records = new ArrayList<>();
        DimensionRecord r;
        while ((r = reader.read()) != null) records.add(r);
        reader.close();

        assertThat(records).hasSize(2);
        assertThat(records.get(0).csiId()).isEqualTo("E");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private Path writeFile(String name, String content) throws IOException {
        Path p = tmp.resolve(name);
        Files.writeString(p, content, StandardCharsets.UTF_8);
        return p;
    }

    private List<DimensionRecord> readAll(PartitionedFileItemReader reader) throws Exception {
        reader.open(new ExecutionContext());
        List<DimensionRecord> result = new ArrayList<>();
        DimensionRecord r;
        while ((r = reader.read()) != null) result.add(r);
        reader.close();
        return result;
    }
}
