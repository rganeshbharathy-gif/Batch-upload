package com.example.batchupload.reader;

import com.example.batchupload.model.DimensionRecord;
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

/**
 * Reads a specific byte range of a pipe-delimited flat file.
 *
 * <p><b>Byte-boundary handling:</b>
 * <ul>
 *   <li>Seeks the {@link FileChannel} directly to {@code startByte} — O(1), no data reading.
 *   <li>If {@code startByte > 0} it skips the first (partial) line so only the pod that
 *       owns the previous range is responsible for that line.
 *   <li>Reads until the current line-based byte cursor exceeds {@code endByte},
 *       except for the last pod which reads to EOF.
 * </ul>
 *
 * <p><b>Column extraction:</b> Column indices are zero-based positions in the
 * pipe-delimited row. They are configurable via {@code application.yml} so that
 * the implementation never needs to be changed when the upstream file layout changes.
 */
public class ByteRangeFlatFileItemReader extends AbstractItemStreamItemReader<DimensionRecord> {

    private static final Logger log = LoggerFactory.getLogger(ByteRangeFlatFileItemReader.class);
    private static final int BUFFER_SIZE = 128 * 1024; // 128 KB per reader

    private final Path filePath;
    private final FileRange range;
    private final int csiIdIndex;
    private final int personIdIndex;
    private final int countryCodeIndex;
    private final int economicCodeIndex;

    private FileChannel fileChannel;
    private BufferedReader reader;
    private long byteCursor;
    private long linesRead;
    private long linesSkipped;

    public ByteRangeFlatFileItemReader(
            Path filePath,
            FileRange range,
            int csiIdIndex,
            int personIdIndex,
            int countryCodeIndex,
            int economicCodeIndex) {
        this.filePath = filePath;
        this.range = range;
        this.csiIdIndex = csiIdIndex;
        this.personIdIndex = personIdIndex;
        this.countryCodeIndex = countryCodeIndex;
        this.economicCodeIndex = economicCodeIndex;
        setName(ByteRangeFlatFileItemReader.class.getSimpleName());
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            fileChannel = FileChannel.open(filePath, StandardOpenOption.READ);
            fileChannel.position(range.startByte());

            reader = new BufferedReader(
                    new InputStreamReader(Channels.newInputStream(fileChannel), StandardCharsets.UTF_8),
                    BUFFER_SIZE);

            byteCursor = range.startByte();

            // Skip the partial line that "belongs" to the previous pod
            if (range.startByte() > 0) {
                String partial = reader.readLine();
                if (partial != null) {
                    byteCursor += partial.length() + 1;
                }
            }

            log.info("Opened reader — file={} startByte={} endByte={} isLast={}",
                    filePath, range.startByte(), range.endByte(), range.isLast());

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
            if (!range.isLast() && byteCursor >= range.endByte()) {
                log.debug("Reached end boundary {} at cursor {}", range.endByte(), byteCursor);
                return null;
            }

            String line = reader.readLine();
            if (line == null) {
                return null;
            }

            byteCursor += line.length() + 1;

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

        int maxIndex = Math.max(Math.max(csiIdIndex, personIdIndex),
                Math.max(countryCodeIndex, economicCodeIndex));

        if (fields.length <= maxIndex) {
            log.warn("Skipping malformed line (only {} fields, need at least {}): [{}...]",
                    fields.length, maxIndex + 1, truncate(line, 120));
            return null;
        }

        return new DimensionRecord(
                trim(fields[csiIdIndex]),
                trim(fields[personIdIndex]),
                trim(fields[countryCodeIndex]),
                trim(fields[economicCodeIndex]));
    }

    private static String trim(String value) {
        return value == null ? null : value.strip();
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
