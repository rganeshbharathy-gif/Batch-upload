package com.example.batchupload.reader;

import com.example.batchupload.model.FileLayout;
import com.example.batchupload.model.FileLayout.ColumnIndices;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Scans the daily input file once, up-front, to locate the data region and
 * resolve the positions of the four required columns from the header row.
 *
 * <p>The file is known to have the following structure:
 * <ol>
 *   <li>Line 1 — vendor metadata (ignored).</li>
 *   <li>Line 2 — pipe-delimited header with column names.</li>
 *   <li>Lines 3…N-1 — pipe-delimited data rows.</li>
 *   <li>Line N — footer with a row count (ignored).</li>
 * </ol>
 *
 * <p>The scanner does not load the whole file; it reads only a small prefix
 * to skip the metadata and parse the header, and a small suffix (16 KiB) to
 * locate the byte offset where the footer begins.
 */
public final class FileLayoutScanner {

    /** Bytes to read from the end of the file when searching for the footer's start. */
    private static final int TAIL_BYTES = 16 * 1024;

    private final Path path;
    private final String gridIdHeader;
    private final String personIdHeader;
    private final String countryCodeHeader;
    private final String ecoSectorCodeHeader;

    public FileLayoutScanner(Path path,
                             String gridIdHeader,
                             String personIdHeader,
                             String countryCodeHeader,
                             String ecoSectorCodeHeader) {
        this.path = path;
        this.gridIdHeader = gridIdHeader;
        this.personIdHeader = personIdHeader;
        this.countryCodeHeader = countryCodeHeader;
        this.ecoSectorCodeHeader = ecoSectorCodeHeader;
    }

    public FileLayout scan() throws IOException {
        long fileSize = Files.size(path);
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            // Skip metadata line
            String metadata = raf.readLine();
            if (metadata == null) {
                throw new IOException("File is empty: " + path);
            }

            // Read header line
            long headerStart = raf.getFilePointer();
            String header = raf.readLine();
            if (header == null) {
                throw new IOException("File has no header row: " + path);
            }
            long dataStart = raf.getFilePointer();

            // RandomAccessFile.readLine decodes as ISO-8859-1. For ASCII header
            // tokens this is safe; we only need the bytes to be positionally correct.
            ColumnIndices indices = resolveColumns(header);

            long dataEnd = locateFooterStart(fileSize);
            if (dataEnd <= dataStart) {
                throw new IOException("Computed empty data region (dataStart=" + dataStart
                        + ", dataEnd=" + dataEnd + ") in file: " + path);
            }

            return new FileLayout(dataStart, dataEnd, indices);
        }
    }

    private ColumnIndices resolveColumns(String headerLine) {
        String[] cols = headerLine.split("\\|", -1);
        int grid = -1, person = -1, country = -1, eco = -1;
        for (int i = 0; i < cols.length; i++) {
            String name = cols[i].strip();
            if (name.equalsIgnoreCase(gridIdHeader))        grid = i;
            else if (name.equalsIgnoreCase(personIdHeader))       person = i;
            else if (name.equalsIgnoreCase(countryCodeHeader))    country = i;
            else if (name.equalsIgnoreCase(ecoSectorCodeHeader))  eco = i;
        }
        require(grid    >= 0, gridIdHeader,        headerLine);
        require(person  >= 0, personIdHeader,      headerLine);
        require(country >= 0, countryCodeHeader,   headerLine);
        require(eco     >= 0, ecoSectorCodeHeader, headerLine);
        return new ColumnIndices(grid, person, country, eco);
    }

    private static void require(boolean ok, String name, String header) {
        if (!ok) {
            throw new IllegalStateException("Required column '" + name
                    + "' not found in header row: " + header);
        }
    }

    /**
     * Returns the absolute byte offset at which the footer row begins.
     * Strategy: read the tail of the file, strip any trailing newline, then find
     * the newline that precedes the final (footer) line.
     */
    private long locateFooterStart(long fileSize) throws IOException {
        int tail = (int) Math.min(TAIL_BYTES, fileSize);
        long tailStart = fileSize - tail;

        ByteBuffer buf = ByteBuffer.allocate(tail);
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ch.position(tailStart);
            int read = 0;
            while (read < tail) {
                int n = ch.read(buf);
                if (n < 0) break;
                read += n;
            }
        }
        byte[] bytes = buf.array();
        int len = bytes.length;

        // Trim trailing newline / CRLF so we find the newline that *precedes* the footer.
        int end = len;
        while (end > 0 && (bytes[end - 1] == '\n' || bytes[end - 1] == '\r')) {
            end--;
        }

        // Scan backwards for the newline that separates the last non-empty line from the rest.
        int idx = -1;
        for (int i = end - 1; i >= 0; i--) {
            if (bytes[i] == '\n') { idx = i; break; }
        }
        if (idx < 0) {
            throw new IOException("Could not locate footer boundary in tail of file: "
                    + path + " (tail=" + new String(bytes, 0, len, StandardCharsets.UTF_8) + ")");
        }
        // The footer line starts at the byte after this newline.
        return tailStart + idx + 1;
    }
}
