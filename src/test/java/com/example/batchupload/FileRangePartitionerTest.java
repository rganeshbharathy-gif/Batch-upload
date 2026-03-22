package com.example.batchupload;

import com.example.batchupload.partitioner.FileRangePartitioner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.item.ExecutionContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FileRangePartitioner}.
 */
class FileRangePartitionerTest {

    @TempDir
    Path tmp;

    @Test
    void partitionsDoNotOverlap() throws IOException {
        // 10 data lines, 1 header
        StringBuilder sb = new StringBuilder("header\n");
        for (int i = 0; i < 10; i++) sb.append("line").append(i).append("\n");
        Path file = writeFile("input.txt", sb.toString());

        FileRangePartitioner partitioner = new FileRangePartitioner(file.toString(), true);
        Map<String, ExecutionContext> partitions = partitioner.partition(5);

        assertThat(partitions).isNotEmpty();

        // Collect all ranges and verify they are contiguous and non-overlapping
        long prevEnd = -1;
        for (ExecutionContext ctx : partitions.values()) {
            long start = ctx.getLong(FileRangePartitioner.KEY_START);
            long end   = ctx.getLong(FileRangePartitioner.KEY_END);
            assertThat(start).isLessThan(end);
            if (prevEnd >= 0) {
                assertThat(start).isEqualTo(prevEnd);
            }
            prevEnd = end;
        }

        // Last partition must end at EOF
        long fileSize = Files.size(file);
        assertThat(prevEnd).isEqualTo(fileSize);
    }

    @Test
    void singlePartitionCoverFullFile() throws IOException {
        String content = "A|B|C|D\nE|F|G|H\n";
        Path file = writeFile("small.txt", content);
        long fileSize = Files.size(file);

        FileRangePartitioner partitioner = new FileRangePartitioner(file.toString(), false);
        Map<String, ExecutionContext> partitions = partitioner.partition(1);

        assertThat(partitions).hasSize(1);
        ExecutionContext ctx = partitions.values().iterator().next();
        assertThat(ctx.getLong(FileRangePartitioner.KEY_START)).isEqualTo(0L);
        assertThat(ctx.getLong(FileRangePartitioner.KEY_END)).isEqualTo(fileSize);
    }

    @Test
    void headerIsExcludedFromAllPartitions() throws IOException {
        String header = "col1|col2|col3|col4\n";
        String data   = "A|B|C|D\nE|F|G|H\n";
        Path file = writeFile("headed.txt", header + data);

        long headerBytes = header.getBytes(StandardCharsets.UTF_8).length;

        FileRangePartitioner partitioner = new FileRangePartitioner(file.toString(), true);
        Map<String, ExecutionContext> partitions = partitioner.partition(2);

        for (ExecutionContext ctx : partitions.values()) {
            assertThat(ctx.getLong(FileRangePartitioner.KEY_START))
                    .isGreaterThanOrEqualTo(headerBytes);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private Path writeFile(String name, String content) throws IOException {
        Path p = tmp.resolve(name);
        Files.writeString(p, content, StandardCharsets.UTF_8);
        return p;
    }
}
