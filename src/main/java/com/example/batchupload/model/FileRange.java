package com.example.batchupload.model;

/**
 * A byte range within the input file's <em>data region</em> (i.e. between the
 * header row and the footer row). Assigned to a single pod or worker thread.
 *
 * @param startByte  Inclusive start byte offset (absolute file offset, 0-based).
 * @param endByte    Exclusive end byte offset (absolute file offset).
 * @param isLast     True when this range is the final one in its partitioning level
 *                   (pod or thread). The last range reads to {@code endByte} even
 *                   if the final record crosses the boundary by a few bytes.
 */
public record FileRange(long startByte, long endByte, boolean isLast) {

    /**
     * Calculate the byte range for a specific pod out of {@code totalPods},
     * within the supplied {@code [dataStart, dataEnd)} data region.
     */
    public static FileRange forPod(long dataStart, long dataEnd, int podIndex, int totalPods) {
        long length = dataEnd - dataStart;
        long chunk = length / totalPods;
        long start = dataStart + (chunk * podIndex);
        boolean last = (podIndex == totalPods - 1);
        long end = last ? dataEnd : dataStart + (chunk * (podIndex + 1));
        return new FileRange(start, end, last);
    }

    /** Approximate number of bytes this range covers. */
    public long length() {
        return endByte - startByte;
    }
}
