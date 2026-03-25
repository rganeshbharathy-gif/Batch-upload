package com.example.batchupload.processor;

import com.example.batchupload.model.DimensionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

/**
 * Validates and sanitises each {@link DimensionRecord} before it reaches the writer.
 *
 * <p>Returning {@code null} causes Spring Batch to silently skip the item — no exception,
 * no retry, no rollback. Use this for business-rule filtering (e.g. blank mandatory fields).
 */
@Component
public class DimensionItemProcessor implements ItemProcessor<DimensionRecord, DimensionRecord> {

    private static final Logger log = LoggerFactory.getLogger(DimensionItemProcessor.class);

    @Override
    public DimensionRecord process(DimensionRecord item) {
        if (isBlank(item.gridId()) && isBlank(item.personId())) {
            log.debug("Skipping record — both grid_id and person_id are blank");
            return null;
        }

        return new DimensionRecord(
                truncate(item.gridId(), 100),
                truncate(item.personId(), 100),
                truncate(item.countryCode(), 10),
                truncate(item.sectorCode(), 50),
                item.podIndex()
        );
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() > maxLen ? s.substring(0, maxLen) : s;
    }
}
