package com.example.batchupload.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Divides a large flat file into {@code gridSize} non-overlapping byte ranges,
 * each aligned to a line boundary.
 *
 * <h2>Algorithm</h2>
 * <ol>
 *   <li>Optionally skip the header line and note its end position.</li>
 *   <li>Calculate {@code gridSize - 1} evenly-spaced approximate cut points
 *       across the remaining file bytes.</li>
 *   <li>For each cut point, seek with {@link RandomAccessFile} (O(1)) and
 *       advance forward byte-by-byte until a {@code '\n'} is found, snapping
 *       to a real line boundary. At most one line-length of extra bytes is
 *       read per partition – negligible for a 20 GB file.</li>
 * </ol>
 *
 * <h2>ExecutionContext keys written per partition</h2>
 * <ul>
 *   <li>{@code startOffset} – inclusive byte offset to seek to before reading</li>
 *   <li>{@code endOffset}   – exclusive byte offset; reading stops here</li>
 *   <li>{@code partitionIndex} – 0-based index for logging</li>
 * </ul>
 */
public class FileRangePartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(FileRangePartitioner.class);

    /** Key written to each partition's ExecutionContext – inclusive start byte. */
    public static final String KEY_START = "startOffset";

    /** Key written to each partition's ExecutionContext – exclusive end byte. */
    public static final String KEY_END   = "endOffset";

    /** Key written to each partition's ExecutionContext – 0-based partition index. */
    public static final String KEY_INDEX = "partitionIndex";

    private final String filePath;
    private final boolean hasHeader;

    public FileRangePartitioner(String filePath, boolean hasHeader) {
        this.filePath  = filePath;
        this.hasHeader = hasHeader;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        long fileSize = new File(filePath).length();
        long[] boundaries = computeBoundaries(filePath, fileSize, gridSize, hasHeader);

        Map<String, ExecutionContext> partitions = new LinkedHashMap<>(gridSize);
        for (int i = 0; i < boundaries.length - 1; i++) {
            ExecutionContext ctx = new ExecutionContext();
            ctx.putLong(KEY_START, boundaries[i]);
            ctx.putLong(KEY_END,   boundaries[i + 1]);
            ctx.putInt(KEY_INDEX,  i);
            partitions.put("partition:" + i, ctx);

            log.debug("Partition {} → bytes [{}, {})", i, boundaries[i], boundaries[i + 1]);
        }

        log.info("File {} ({} bytes) split into {} partitions", filePath, fileSize, partitions.size());
        return partitions;
    }

    /**
     * Returns an array of {@code gridSize + 1} byte offsets:
     * {@code [dataStart, cut1, cut2, ..., fileSize]}.
     * Each offset points to the first byte of a new line.
     */
    private static long[] computeBoundaries(String filePath, long fileSize,
                                             int gridSize, boolean hasHeader) {
        long[] boundaries = new long[gridSize + 1];

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {

            // ── boundary[0]: start of data (skip header if present) ──────────
            long dataStart = 0L;
            if (hasHeader) {
                skipToNextLine(raf); // reads header line (byte-by-byte; once only)
                dataStart = raf.getFilePointer();
            }
            boundaries[0] = dataStart;

            long dataSize = fileSize - dataStart;

            // ── boundaries[1..gridSize-1]: evenly-spaced, snapped to line ends ─
            for (int i = 1; i < gridSize; i++) {
                long approx = dataStart + (long) i * dataSize / gridSize;
                raf.seek(approx);
                skipToNextLine(raf); // advance past partial line → next line start
                long snapped = raf.getFilePointer();

                // If we've run off the end, all remaining partitions collapse to EOF
                boundaries[i] = Math.min(snapped, fileSize);
            }

            // ── boundary[gridSize]: end of file ───────────────────────────────
            boundaries[gridSize] = fileSize;

        } catch (IOException e) {
            throw new IllegalStateException("Cannot partition file: " + filePath, e);
        }

        // Deduplicate: if two consecutive boundaries are equal the file is smaller
        // than gridSize lines – reduce to actual distinct ranges.
        return dedup(boundaries);
    }

    /**
     * Reads bytes from the current position until {@code '\n'} or EOF.
     * After the call the file pointer is positioned at the first byte of the
     * next line (or at EOF).
     */
    private static void skipToNextLine(RandomAccessFile raf) throws IOException {
        int b;
        while ((b = raf.read()) != -1) {
            if (b == '\n') break;
        }
    }

    /**
     * Removes duplicate consecutive boundaries that arise when the file has
     * fewer lines than requested partitions, returning a compacted array that
     * still starts with {@code original[0]} and ends with the last element.
     */
    private static long[] dedup(long[] boundaries) {
        int unique = 1;
        for (int i = 1; i < boundaries.length; i++) {
            if (boundaries[i] != boundaries[unique - 1]) {
                boundaries[unique++] = boundaries[i];
            }
        }
        if (unique == boundaries.length) return boundaries;

        long[] result = new long[unique];
        System.arraycopy(boundaries, 0, result, 0, unique);
        return result;
    }
}
