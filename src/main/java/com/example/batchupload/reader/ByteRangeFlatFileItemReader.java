package com.example.batchupload.reader;

import com.example.batchupload.model.DimensionRecord;
import com.example.batchupload.model.FileRange;
import com.example.batchupload.service.S3FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.support.AbstractItemStreamItemReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads a specific byte range of a pipe-delimited file stored in S3.
 *
 * <p><b>Header-based column discovery:</b> Pod 0 reads the metadata line and header
 * line at the top of the file, then builds a column-name-to-index map so that
 * column positions are never hardcoded. Non-zero pods skip partial lines as before.
 *
 * <p><b>Byte-boundary handling:</b>
 * <ul>
 *   <li>Uses S3's native byte-range GET so each pod fetches only its slice.
 *   <li>If {@code startByte > 0} it skips the first (partial) line.
 *   <li>Reads until the byte cursor exceeds {@code endByte}, except for the
 *       last pod which reads to EOF.
 * </ul>
 */
public class ByteRangeFlatFileItemReader extends AbstractItemStreamItemReader<DimensionRecord> {

    private static final Logger log = LoggerFactory.getLogger(ByteRangeFlatFileItemReader.class);
    private static final int BUFFER_SIZE = 128 * 1024; // 128 KB per reader

    private final S3FileService s3FileService;
    private final FileRange range;
    private final String bucket;
    private final String s3Key;
    private final Path localFilePath;

    private InputStream s3InputStream;
    private BufferedReader reader;
    private long byteCursor;
    private long linesRead;
    private long linesSkipped;
    private long expectedRowCount = -1;  // extracted from footer by last pod

    // Column indices resolved from the header row
    private int gridIdIndex;
    private int personIdIndex;
    private int countryCodeIndex;
    private int sectorCodeIndex;

    /** S3-based constructor — resolves column indices from the file header. */
    public ByteRangeFlatFileItemReader(S3FileService s3FileService, FileRange range,
                                       String bucket, String s3Key) {
        this.s3FileService = s3FileService;
        this.range = range;
        this.bucket = bucket;
        this.s3Key = s3Key;
        this.localFilePath = null;
        setName(ByteRangeFlatFileItemReader.class.getSimpleName());
    }

    /** Local-file constructor for testing — column indices are provided directly. */
    public ByteRangeFlatFileItemReader(Path localFilePath, FileRange range,
                                       int gridIdIndex, int personIdIndex,
                                       int countryCodeIndex, int sectorCodeIndex) {
        this.s3FileService = null;
        this.range = range;
        this.bucket = null;
        this.s3Key = null;
        this.localFilePath = localFilePath;
        this.gridIdIndex = gridIdIndex;
        this.personIdIndex = personIdIndex;
        this.countryCodeIndex = countryCodeIndex;
        this.sectorCodeIndex = sectorCodeIndex;
        setName(ByteRangeFlatFileItemReader.class.getSimpleName());
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            if (localFilePath != null) {
                // Local file mode (testing) — column indices already set via constructor
                openLocalFile();
            } else if (range.startByte() == 0) {
                // Pod 0: read from the beginning so we can parse metadata + header
                s3InputStream = s3FileService.getInputStream(bucket, s3Key, 0, range.endByte());
                reader = new BufferedReader(
                        new InputStreamReader(s3InputStream, StandardCharsets.UTF_8), BUFFER_SIZE);

                byteCursor = 0;
                parseHeader();
            } else {
                // Non-zero pods: need the header from the start of the file first
                resolveHeaderFromFileStart();

                // Now open the actual byte range for data reading
                s3InputStream = s3FileService.getInputStream(bucket, s3Key, range.startByte(), range.endByte());
                reader = new BufferedReader(
                        new InputStreamReader(s3InputStream, StandardCharsets.UTF_8), BUFFER_SIZE);

                byteCursor = range.startByte();

                // Skip the partial line that "belongs" to the previous pod
                String partial = reader.readLine();
                if (partial != null) {
                    byteCursor += partial.length() + 1;
                }
            }

            log.info("Opened reader — startByte={} endByte={} isLast={} columns=[GRID_ID={}, PERSON_ID={}, COUNTRY_CODE={}, SECTOR_CODE={}]",
                    range.startByte(), range.endByte(), range.isLast(),
                    gridIdIndex, personIdIndex, countryCodeIndex, sectorCodeIndex);

        } catch (IOException e) {
            throw new ItemStreamException("Cannot open S3 stream", e);
        }
    }

    /**
     * Reads the first two lines of the file (metadata + header) to discover column indices.
     * Called inline for pod 0 since it already reads from byte 0.
     */
    private void parseHeader() throws IOException {
        // Line 1: metadata — skip it
        String metaData = reader.readLine();
        if (metaData != null) {
            byteCursor += metaData.length() + 1;
        }

        // Line 2: header row with column names
        String header = reader.readLine();
        if (header == null) {
            throw new IOException("File has no header row");
        }
        byteCursor += header.length() + 1;

        resolveColumnIndices(header);
    }

    /**
     * Opens a local file for testing — skips partial first line for non-zero ranges.
     * Column indices are already set via constructor, so no header parsing is needed.
     */
    private void openLocalFile() throws IOException {
        s3InputStream = java.nio.file.Files.newInputStream(localFilePath);
        // Skip to startByte
        long skipped = s3InputStream.skip(range.startByte());
        if (skipped != range.startByte()) {
            throw new IOException("Could not skip to byte " + range.startByte());
        }
        reader = new BufferedReader(
                new InputStreamReader(s3InputStream, StandardCharsets.UTF_8), BUFFER_SIZE);
        byteCursor = range.startByte();

        if (range.startByte() > 0) {
            // Skip the partial line that "belongs" to the previous pod
            String partial = reader.readLine();
            if (partial != null) {
                byteCursor += partial.length() + 1;
            }
        }
    }

    /**
     * For non-zero pods: makes a small S3 range request to read just the header,
     * then closes that stream before opening the real data stream.
     */
    private void resolveHeaderFromFileStart() throws IOException {
        // Read enough bytes to cover metadata + header (first 8 KB should be plenty)
        try (InputStream headerStream = s3FileService.getInputStream(bucket, s3Key, 0, 8192);
             BufferedReader headerReader = new BufferedReader(
                     new InputStreamReader(headerStream, StandardCharsets.UTF_8))) {

            headerReader.readLine(); // skip metadata
            String header = headerReader.readLine();
            if (header == null) {
                throw new IOException("File has no header row");
            }
            resolveColumnIndices(header);
        }
    }

    private void resolveColumnIndices(String header) {
        String[] columns = header.split("\\|", -1);
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            idx.put(columns[i].trim(), i);
        }

        gridIdIndex = requireColumn(idx, "GRID_ID");
        personIdIndex = requireColumn(idx, "PERSON_ID");
        countryCodeIndex = requireColumn(idx, "COUNTRY_CODE");
        sectorCodeIndex = requireColumn(idx, "SECTOR_CODE");
    }

    private static int requireColumn(Map<String, Integer> idx, String name) {
        Integer index = idx.get(name);
        if (index == null) {
            throw new ItemStreamException("Required column '" + name + "' not found in header. Available: " + idx.keySet());
        }
        return index;
    }

    @Override
    public void update(ExecutionContext executionContext) {
        executionContext.putLong("byteCursor", byteCursor);
        executionContext.putLong("linesRead", linesRead);
        if (expectedRowCount >= 0) {
            executionContext.putLong("footer.expectedRowCount", expectedRowCount);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (reader != null) reader.close();
            if (s3InputStream != null) s3InputStream.close();
            log.info("Closed S3 reader — linesRead={} linesSkipped={} finalCursor={}",
                    linesRead, linesSkipped, byteCursor);
        } catch (IOException e) {
            throw new ItemStreamException("Cannot close S3 stream", e);
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

            // Last pod: check if this line is the footer (last line before EOF)
            if (range.isLast()) {
                reader.mark(BUFFER_SIZE);
                String nextLine = reader.readLine();
                if (nextLine == null) {
                    // This line is the footer — extract expected row count from index 1
                    parseFooter(line);
                    return null;
                }
                reader.reset();
            }

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

    /**
     * Parses the footer line to extract the expected row count.
     * Footer format: {@code FOOTER|12345|...} — index 1 (2nd field) is the row count.
     */
    private void parseFooter(String footerLine) {
        try {
            String[] fields = footerLine.split("\\|", -1);
            if (fields.length >= 2) {
                expectedRowCount = Long.parseLong(fields[1].trim());
                log.info("Footer detected — expected row count: {}", expectedRowCount);
            } else {
                log.warn("Footer line has fewer than 2 fields: [{}]", truncate(footerLine, 120));
            }
        } catch (NumberFormatException e) {
            log.warn("Could not parse row count from footer: [{}]", truncate(footerLine, 120));
        }
    }

    private DimensionRecord parseLine(String line) {
        String[] fields = line.split("\\|", -1);

        int maxIndex = Math.max(Math.max(gridIdIndex, personIdIndex),
                Math.max(countryCodeIndex, sectorCodeIndex));

        if (fields.length <= maxIndex) {
            log.warn("Skipping malformed line (only {} fields, need at least {}): [{}...]",
                    fields.length, maxIndex + 1, truncate(line, 120));
            return null;
        }

        return new DimensionRecord(
                trim(fields[personIdIndex]),
                trim(fields[gridIdIndex]),
                trim(fields[countryCodeIndex]),
                trim(fields[sectorCodeIndex]));
    }

    private static String trim(String value) {
        return value == null ? null : value.strip();
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
