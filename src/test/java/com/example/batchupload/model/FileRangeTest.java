package com.example.batchupload.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FileRangeTest {

    @Test
    void forPod_shouldSplitEvenly() {
        long fileSize = 20_000_000_000L;
        int totalPods = 5;
        long coveredBytes = 0;
        for (int i = 0; i < totalPods; i++) {
            coveredBytes += FileRange.forPod(fileSize, i, totalPods).length();
        }
        assertThat(coveredBytes).isEqualTo(fileSize);
    }

    @Test
    void forPod_lastPodGetsRemainder() {
        long fileSize = 103L;
        int totalPods = 5;
        FileRange last = FileRange.forPod(fileSize, 4, totalPods);
        assertThat(last.endByte()).isEqualTo(fileSize);
        assertThat(last.length()).isGreaterThanOrEqualTo(fileSize / totalPods);
    }

    @Test
    void forPod_singlePod() {
        long fileSize = 5000L;
        FileRange range = FileRange.forPod(fileSize, 0, 1);
        assertThat(range.startByte()).isZero();
        assertThat(range.endByte()).isEqualTo(fileSize);
        assertThat(range.isLast()).isTrue();
    }

    @Test
    void forPod_onlyLastPodIsMarkedLast() {
        int totalPods = 3;
        for (int i = 0; i < totalPods - 1; i++) {
            assertThat(FileRange.forPod(10000L, i, totalPods).isLast()).isFalse();
        }
        assertThat(FileRange.forPod(10000L, totalPods - 1, totalPods).isLast()).isTrue();
    }

    @Test
    void length_returnsEndMinusStart() {
        FileRange range = new FileRange(100, 500, false);
        assertThat(range.length()).isEqualTo(400);
    }
}
