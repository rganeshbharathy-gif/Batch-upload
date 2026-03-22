package com.example.batchupload.partitioner;

import com.example.batchupload.model.FileRange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Divides the pod's assigned byte range further into {@code gridSize} sub-ranges,
 * one per worker thread.
 *
 * <p>This runs inside a single pod. The outer Kubernetes-level split is handled by
 * {@link com.example.batchupload.config.BatchConfig} reading {@code JOB_COMPLETION_INDEX}
 * and {@code TOTAL_PODS} environment variables.
 */
@Slf4j
@RequiredArgsConstructor
public class PodByteRangePartitioner implements Partitioner {

    private final Path filePath;

    /** Byte range assigned to THIS pod (already pre-calculated from K8s env vars). */
    private final FileRange podRange;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        long rangeStart = podRange.startByte();
        long rangeEnd = podRange.isLast() ? fileSize() : podRange.endByte();
        long rangeLength = rangeEnd - rangeStart;
        long subChunkSize = rangeLength / gridSize;

        Map<String, ExecutionContext> partitions = new HashMap<>();

        for (int i = 0; i < gridSize; i++) {
            long subStart = rangeStart + (subChunkSize * i);
            boolean isLastSubPartition = (i == gridSize - 1);
            long subEnd = isLastSubPartition ? rangeEnd : (subStart + subChunkSize);

            FileRange subRange = new FileRange(subStart, subEnd, isLastSubPartition && podRange.isLast());

            ExecutionContext ctx = new ExecutionContext();
            ctx.putLong("startByte", subRange.startByte());
            ctx.putLong("endByte", subRange.endByte());
            ctx.putString("isLast", String.valueOf(subRange.isLast()));
            ctx.putInt("partitionIndex", i);

            String partitionKey = "partition-" + i;
            partitions.put(partitionKey, ctx);

            log.info("Sub-partition {} — [{}, {}), isLast={}",
                    partitionKey, subStart, subEnd, subRange.isLast());
        }

        return partitions;
    }

    private long fileSize() {
        try {
            return Files.size(filePath);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot determine file size for: " + filePath, e);
        }
    }
}
