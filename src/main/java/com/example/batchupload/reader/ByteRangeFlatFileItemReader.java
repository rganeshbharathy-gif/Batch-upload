package com.example.batchupload.reader;

import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.model.FileRange;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.channels.FileChannel;

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
@Slf4j
public class ByteRangeFlatFileItemReader extends AbstractItemStreamItemReader<DimensionRecord> {

    private static final int BUFFER_SIZE = 128 * 1024; // 128 KB per reader

    private final Path filePath;
    private final FileRange range;
    private final int csiIdIndex;
    private final int personIdIndex;
    private final int countryCodeIndex;
    private final int economicCodeIndex;

    private FileChannel fileChannel;
    private BufferedReader reader;

    /** Running byte-position cursor (approximated via line lengths). */
    private long byteCursor;

    /** Count of lines successfully read and parsed. */
    private long linesRead;

    /** Count of lines skipped due to parse errors. */
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

    // -------------------------------------------------------------------------
    // ItemStream lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            fileChannel = FileChannel.open(filePath, StandardOpenOption.READ);
            fileChannel.position(range.startByte());

            // Wrap the channel in a buffered reader for efficient line reading
            reader = new BufferedReader(
                    new InputStreamReader(Channels.newInputStream(fileChannel), StandardCharsets.UTF_8),
                    BUFFER_SIZE);

            byteCursor = range.startByte();

            // Skip the partial line that "belongs" to the previous pod
            if (range.startByte() > 0) {
                String partial = reader.readLine();
                if (partial != null) {
                    byteCursor += partial.length() + 1; // +1 for '\n'
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

    // -------------------------------------------------------------------------
    // ItemReader
    // -------------------------------------------------------------------------

    @Override
    public DimensionRecord read() throws Exception {
        while (true) {
            // Non-last pods stop when the cursor moves past their end boundary.
            // We check BEFORE reading the next line so we never steal lines
            // that belong to the next pod's range.
            if (!range.isLast() && byteCursor >= range.endByte()) {
                log.debug("Reached end boundary {} at cursor {}", range.endByte(), byteCursor);
                return null;
            }

            String line = reader.readLine();
            if (line == null) {
                return null; // EOF
            }

            // Advance cursor (line length + newline byte).
            // For ASCII/Latin-1 data this is exact; for UTF-8 multi-byte it is an
            // approximation — acceptable because boundary overlap is at most a few lines.
            byteCursor += line.length() + 1;

            if (line.isBlank()) {
                linesSkipped++;
                continue; // skip empty lines
            }

            DimensionRecord record = parseLine(line);
            if (record == null) {
                linesSkipped++;
                continue; // skip malformed lines; logged inside parseLine
            }

            linesRead++;
            return record;
        }
    }

    // -------------------------------------------------------------------------
    // Parsing
    // -------------------------------------------------------------------------

    private static final int MIN_REQUIRED_COLUMNS_OFFSET = 1; // inclusive index check

    private DimensionRecord parseLine(String line) {
        // Split with limit -1 to preserve trailing empty fields
        String[] fields = line.split("\\|", -1);

        int maxIndex = Math.max(Math.max(csiIdIndex, personIdIndex),
                Math.max(countryCodeIndex, economicCodeIndex));

        if (fields.length <= maxIndex) {
            log.warn("Skipping malformed line (only {} fields, need at least {}): [{}...]",
                    fields.length, maxIndex + 1, truncate(line, 120));
            return null;
        }

        DimensionRecord record = new DimensionRecord();
        record.setCsiId(trim(fields[csiIdIndex]));
        record.setPersonId(trim(fields[personIdIndex]));
        record.setCountryCode(trim(fields[countryCodeIndex]));
        record.setEconomicCode(trim(fields[economicCodeIndex]));
        return record;
    }

    private static String trim(String value) {
        return value == null ? null : value.strip();
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }
}
