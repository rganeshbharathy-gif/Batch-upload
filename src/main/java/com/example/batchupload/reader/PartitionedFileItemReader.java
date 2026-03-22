package com.example.batchupload.reader;

import com.example.batchupload.config.BatchProperties.ColumnIndices;
import com.example.batchupload.model.DimensionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/**
 * High-throughput, partition-aware reader for a single byte range of a large
 * pipe-delimited flat file.
 *
 * <h2>Design decisions</h2>
 * <ul>
 *   <li><strong>Byte-range reading</strong> – uses {@link FileInputStream}
 *       with NIO {@code channel().position(startOffset)} for an O(1) seek to
 *       the partition start, then wraps the stream in a
 *       {@link LimitedInputStream} that returns EOF after exactly
 *       {@code (endOffset - startOffset)} bytes. Because the partitioner
 *       guarantees both boundaries are on line boundaries, every
 *       {@code readLine()} call returns a complete record.</li>
 *   <li><strong>8 MB {@link BufferedReader} buffer</strong> – amortises
 *       system-call overhead across thousands of lines per I/O operation.</li>
 *   <li><strong>Single-pass column extraction</strong> – rather than
 *       {@code String.split("|")} which allocates a 250-element array per row,
 *       the parser makes one left-to-right pass and stops as soon as the last
 *       required column is consumed. Column targets are sorted by index so
 *       the parser always moves forward.</li>
 *   <li><strong>Restartability</strong> – the number of items successfully
 *       read is persisted in the {@link ExecutionContext}. On restart the
 *       reader re-opens the file at {@code startOffset} and skips that many
 *       lines before resuming normal processing.</li>
 * </ul>
 *
 * <p>Each Spring Batch partition step execution gets its own instance of this
 * reader (via {@code @StepScope} on the factory bean), so no synchronisation
 * is needed – virtual threads each drive an independent reader.
 */
public class PartitionedFileItemReader implements ItemStreamReader<DimensionRecord> {

    private static final Logger log = LoggerFactory.getLogger(PartitionedFileItemReader.class);

    /** Size of the {@link BufferedReader} internal buffer: 8 MB. */
    private static final int READ_BUFFER_BYTES = 8 * 1024 * 1024;

    /** ExecutionContext key used for restartability. */
    private static final String CTX_ITEMS_READ = "items.read";

    // ── Configuration (set at construction time, immutable) ──────────────────

    private final String filePath;
    private final long startOffset;
    private final long endOffset;

    /**
     * Pre-computed sort-ordered mapping: {@code [outputIndex, columnIndex]}.
     * Sorted by {@code columnIndex} so the single-pass parser can advance
     * left-to-right through the line without back-tracking.
     *
     * <p>Output indices map to {@link DimensionRecord} constructor parameters:
     * 0=csiId, 1=personId, 2=countryCode, 3=economicCode.
     */
    private final int[][] sortedColumnMap;

    /** The highest column index we need – the parser can stop after this. */
    private final int maxColumnIndex;

    // ── Mutable state ─────────────────────────────────────────────────────────

    private BufferedReader reader;
    private FileInputStream fis;
    private long itemsRead = 0L;

    // ─────────────────────────────────────────────────────────────────────────

    public PartitionedFileItemReader(String filePath,
                                      long startOffset,
                                      long endOffset,
                                      ColumnIndices columns) {
        this.filePath    = filePath;
        this.startOffset = startOffset;
        this.endOffset   = endOffset;

        // Build and sort the column-to-output-index mapping once.
        int[][] mapping = {
                {0, columns.csiId()},
                {1, columns.personId()},
                {2, columns.countryCode()},
                {3, columns.economicCode()}
        };
        Arrays.sort(mapping, Comparator.comparingInt(a -> a[1]));
        this.sortedColumnMap  = mapping;
        this.maxColumnIndex   = Arrays.stream(mapping).mapToInt(m -> m[1]).max().orElse(0);
    }

    // ── ItemStream ────────────────────────────────────────────────────────────

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        long toSkip = executionContext.getLong(CTX_ITEMS_READ, 0L);
        long limit  = endOffset - startOffset;

        try {
            fis = new FileInputStream(filePath);
            // O(1) seek via NIO FileChannel – no bytes are read.
            fis.getChannel().position(startOffset);

            InputStream bounded = new LimitedInputStream(fis, limit);
            reader = new BufferedReader(
                    new InputStreamReader(bounded, StandardCharsets.UTF_8),
                    READ_BUFFER_BYTES
            );

            // Skip already-processed lines for job restart support.
            if (toSkip > 0) {
                log.debug("Partition [{},{}) – resuming from line {}", startOffset, endOffset, toSkip);
                for (long i = 0; i < toSkip; i++) {
                    if (reader.readLine() == null) {
                        log.warn("Reached EOF before skipping {} lines during restart", toSkip);
                        break;
                    }
                }
            }
            itemsRead = toSkip;

            log.debug("Opened partition [{}, {}) – limit {} bytes, skip {} lines",
                    startOffset, endOffset, limit, toSkip);

        } catch (IOException e) {
            throw new ItemStreamException(
                    "Failed to open file at offset " + startOffset + ": " + filePath, e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putLong(CTX_ITEMS_READ, itemsRead);
    }

    @Override
    public void close() throws ItemStreamException {
        if (reader != null) {
            try {
                reader.close(); // also closes the underlying FileInputStream
            } catch (IOException e) {
                throw new ItemStreamException("Failed to close reader for " + filePath, e);
            }
        }
    }

    // ── ItemReader ────────────────────────────────────────────────────────────

    /**
     * Returns the next {@link DimensionRecord} or {@code null} when the
     * partition's byte range is exhausted.
     *
     * <p>Blank lines are silently skipped.
     */
    @Override
    public DimensionRecord read() throws Exception {
        String line;
        do {
            line = reader.readLine();
            if (line == null) return null; // partition exhausted
        } while (line.isBlank());

        itemsRead++;
        return parseLine(line);
    }

    // ── Parsing ───────────────────────────────────────────────────────────────

    /**
     * Extracts the four required fields from a pipe-delimited line in a single
     * left-to-right pass. Stops as soon as {@code maxColumnIndex} is passed –
     * trailing columns are never visited.
     *
     * <p>If a required column index exceeds the actual number of delimiters on
     * the line the corresponding field defaults to {@code ""} (empty string)
     * to avoid null values reaching the database.
     */
    private DimensionRecord parseLine(String line) {
        String[] values  = new String[4];
        int colNum       = 0;
        int targetIdx    = 0;
        int start        = 0;
        int len          = line.length();

        for (int i = 0; i <= len; i++) {
            // Treat end-of-string as an implicit delimiter to handle last field.
            if (i == len || line.charAt(i) == '|') {

                // Match any targets sitting at colNum (sorted → at most one match
                // unless caller maps two outputs to the same source column).
                while (targetIdx < sortedColumnMap.length
                        && sortedColumnMap[targetIdx][1] == colNum) {
                    int outputIdx = sortedColumnMap[targetIdx][0];
                    values[outputIdx] = line.substring(start, i).trim();
                    targetIdx++;
                }

                // Early exit once all required columns are collected.
                if (targetIdx >= sortedColumnMap.length) break;

                // Abort scan past the furthest required column (skip trailing cols).
                if (colNum >= maxColumnIndex) break;

                colNum++;
                start = i + 1;
            }
        }

        return new DimensionRecord(
                Objects.requireNonNullElse(values[0], ""),
                Objects.requireNonNullElse(values[1], ""),
                Objects.requireNonNullElse(values[2], ""),
                Objects.requireNonNullElse(values[3], "")
        );
    }

    // ── Inner class: LimitedInputStream ──────────────────────────────────────

    /**
     * Wraps an {@link InputStream} and returns EOF after a fixed number of
     * bytes, ensuring the reader never crosses a partition boundary.
     *
     * <p>Because the {@link FileRangePartitioner} guarantees that
     * {@code limit = endOffset - startOffset} ends exactly on a newline, all
     * lines within the limit are complete; no partial records are returned.
     */
    private static final class LimitedInputStream extends FilterInputStream {

        private long remaining;

        LimitedInputStream(InputStream in, long limit) {
            super(in);
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) return -1;
            int b = super.read();
            if (b != -1) remaining--;
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (remaining <= 0) return -1;
            int toRead = (int) Math.min(len, remaining);
            int n = super.read(buf, off, toRead);
            if (n > 0) remaining -= n;
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long toSkip = Math.min(n, remaining);
            long skipped = super.skip(toSkip);
            remaining -= skipped;
            return skipped;
        }
    }
}
