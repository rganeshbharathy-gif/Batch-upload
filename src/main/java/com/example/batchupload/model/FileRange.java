package com.example.batchupload.model;

/**
 * Represents a byte range within the input file assigned to a single pod/thread.
 *
 * @param startByte  Inclusive start byte offset (0-based).
 * @param endByte    Exclusive end byte offset. -1 means "read to EOF".
 * @param isLast     True when this range covers the tail of the file (reads to EOF).
 */
public record FileRange(long startByte, long endByte, boolean isLast) {

    /**
     * Calculate the byte range for a specific pod out of {@code totalPods}.
     *
     * @param fileSize   Total file size in bytes.
     * @param podIndex   Zero-based index of this pod.
     * @param totalPods  Total number of pods sharing the file.
     */
    public static FileRange forPod(long fileSize, int podIndex, int totalPods) {
        long chunkSize = fileSize / totalPods;
        long start = chunkSize * podIndex;
        boolean isLastPod = (podIndex == totalPods - 1);
        long end = isLastPod ? fileSize : chunkSize * (podIndex + 1);
        return new FileRange(start, end, isLastPod);
    }

    /** Approximate number of bytes this range covers. */
    public long length() {
        return endByte - startByte;
    }
}
