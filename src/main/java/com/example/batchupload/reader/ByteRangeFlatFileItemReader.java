package com.example.batchupload.reader;

import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.model.FileLayout.ColumnIndices;
import com.example.batchupload.model.FileRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.support.AbstractItemStreamItemReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Reads a specific byte range of the data region in a pipe-delimited file.
 *
 * <p><b>Responsibilities</b>
 * <ul>
 *   <li>Seeks to {@code startByte}, discarding the first partial line unless
 *       the range begins at the file's {@code dataStart} (guaranteeing every
 *       line is owned by exactly one partition).</li>
 *   <li>Reads until the byte cursor crosses {@code endByte}; the last partition
 *       may read slightly past it to finish the final record.</li>
 *   <li>Extracts the four configured columns by index, constructs a
 *       {@link DimensionRecord}, and stamps it with the shared load timestamp.</li>
 *   <li>Leaves numeric parsing of {@code ecoSectorCode} to the processor so that
 *       a malformed value can be skipped without corrupting the byte cursor.</li>
 * </ul>
 */
public class ByteRangeFlatFileItemReader extends AbstractItemStreamItemReader<DimensionRecord> {

    private static final Logger log = LoggerFactory.getLogger(ByteRangeFlatFileItemReader.class);
    private static final int BUFFER_SIZE = 128 * 1024;

    private final Path filePath;
    private final FileRange range;
    private final long dataStart;
    private final ColumnIndices columns;
    private final Instant loadedAt;

    private FileChannel fileChannel;
    private BufferedReader reader;
    private long byteCursor;
    private long linesRead;
    private long linesSkipped;

    public ByteRangeFlatFileItemReader(Path filePath,
                                       FileRange range,
                                       long dataStart,
                                       ColumnIndices columns,
                                       Instant loadedAt) {
        this.filePath = filePath;
        this.range = range;
        this.dataStart = dataStart;
        this.columns = columns;
        this.loadedAt = loadedAt;
        setName(ByteRangeFlatFileItemReader.class.getSimpleName());
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            fileChannel = FileChannel.open(filePath, StandardOpenOption.READ);

            // If startByte is mid-line, the previous partition's reader will have
            // already consumed that line — we must discard it here. Skip this only
            // when the preceding byte is a newline (the boundary aligned cleanly)
            // or when we're at the very start of the data region.
            boolean skipPartial = range.startByte() > dataStart
                    && !precedingByteIsNewline(fileChannel, range.startByte());

            fileChannel.position(range.startByte());
            reader = new BufferedReader(
                    new InputStreamReader(Channels.newInputStream(fileChannel), StandardCharsets.UTF_8),
                    BUFFER_SIZE);

            byteCursor = range.startByte();
            if (skipPartial) {
                String partial = reader.readLine();
                if (partial != null) {
                    byteCursor += partial.getBytes(StandardCharsets.UTF_8).length + 1;
                }
            }

            log.info("Opened reader — file={} range=[{}, {}) isLast={} dataStart={}",
                    filePath, range.startByte(), range.endByte(), range.isLast(), dataStart);
        } catch (IOException e) {
            throw new ItemStreamException("Cannot open file: " + filePath, e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) {
        executionContext.putLong("byteCursor", byteCursor);
        executionContext.putLong("linesRead", linesRead);
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (reader != null) reader.close();
            if (fileChannel != null) fileChannel.close();
            log.info("Closed reader — linesRead={} linesSkipped={} finalCursor={}",
                    linesRead, linesSkipped, byteCursor);
        } catch (IOException e) {
            throw new ItemStreamException("Cannot close file: " + filePath, e);
        }
    }

    @Override
    public DimensionRecord read() throws Exception {
        while (true) {
            // dataEnd is exact (points at the footer's first byte), and the byte
            // cursor is incremented by exact UTF-8 byte lengths, so every partition
            // — including the last — stops precisely at its endByte boundary.
            if (byteCursor >= range.endByte()) {
                return null;
            }

            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            byteCursor += line.getBytes(StandardCharsets.UTF_8).length + 1;

            if (line.isBlank()) {
                linesSkipped++;
                continue;
            }

            DimensionRecord record = parseLine(line);
            if (record == null) {
                linesSkipped++;
                continue;
            }
            linesRead++;
            return record;
        }
    }

    private DimensionRecord parseLine(String line) {
        String[] fields = line.split("\\|", -1);
        if (fields.length <= columns.maxIndex()) {
            log.warn("Skipping malformed line ({} fields, need {}): [{}]",
                    fields.length, columns.maxIndex() + 1, truncate(line, 120));
            return null;
        }

        String ecoRaw = trim(fields[columns.ecoSectorCode()]);
        Long ecoSector;
        try {
            ecoSector = (ecoRaw == null || ecoRaw.isEmpty()) ? null : Long.parseLong(ecoRaw);
        } catch (NumberFormatException e) {
            log.warn("Skipping line with non-numeric eco_sector_code='{}': [{}]",
                    ecoRaw, truncate(line, 120));
            return null;
        }

        return new DimensionRecord(
                trim(fields[columns.gridId()]),
                trim(fields[columns.personId()]),
                trim(fields[columns.countryCode()]),
                ecoSector,
                loadedAt);
    }

    private static boolean precedingByteIsNewline(FileChannel ch, long pos) throws IOException {
        if (pos <= 0) return false;
        java.nio.ByteBuffer one = java.nio.ByteBuffer.allocate(1);
        ch.position(pos - 1);
        if (ch.read(one) != 1) return false;
        byte b = one.array()[0];
        return b == '\n' || b == '\r';
    }

    private static String trim(String value) {
        return value == null ? null : value.strip();
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }
}
