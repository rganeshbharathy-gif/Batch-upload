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
        String personId = sanitise(item.personId(), 100);

        if (personId == null || personId.isEmpty()) {
            log.debug("Skipping record — person_id (primary key) is null or empty after sanitisation");
            return null;
        }

        return new DimensionRecord(
                personId,
                sanitise(item.gridId(), 100),
                sanitise(item.countryCode(), 10),
                sanitise(item.sectorCode(), 50)
        );
    }

    /**
     * Strips whitespace, truncates to max length, and converts blank strings to null.
     * Oracle treats empty string as NULL, so this ensures consistency between Java and DB.
     */
    private static String sanitise(String s, int maxLen) {
        if (s == null) return null;
        s = s.strip();
        if (s.isEmpty()) return null;
        return s.length() > maxLen ? s.substring(0, maxLen) : s;
    }
}
