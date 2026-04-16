package com.example.batchupload.partitioner;

import com.example.batchupload.model.FileRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Divides this pod's byte range within the data region into {@code gridSize}
 * sub-ranges, one per worker thread. The cross-pod split is resolved before
 * this partitioner runs, using {@code JOB_COMPLETION_INDEX} / {@code TOTAL_PODS}.
 */
public class PodByteRangePartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(PodByteRangePartitioner.class);

    private final FileRange podRange;

    public PodByteRangePartitioner(FileRange podRange) {
        this.podRange = podRange;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        long length = podRange.endByte() - podRange.startByte();
        long subChunk = length / gridSize;
        Map<String, ExecutionContext> partitions = new HashMap<>();

        for (int i = 0; i < gridSize; i++) {
            long subStart = podRange.startByte() + (subChunk * i);
            boolean lastSub = (i == gridSize - 1);
            long subEnd = lastSub ? podRange.endByte() : subStart + subChunk;
            boolean isTailOfData = lastSub && podRange.isLast();

            ExecutionContext ctx = new ExecutionContext();
            ctx.putLong("startByte", subStart);
            ctx.putLong("endByte", subEnd);
            ctx.putString("isLast", String.valueOf(isTailOfData));
            ctx.putInt("partitionIndex", i);

            partitions.put("partition-" + i, ctx);
            log.info("Sub-partition {} — [{}, {}) isLast={}", i, subStart, subEnd, isTailOfData);
        }
        return partitions;
    }
}
