package com.example.batchupload.processor;

import com.example.batchupload.model.DimensionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * Validates and sanitises each {@link DimensionRecord} before it reaches the writer.
 *
 * <p>Returning {@code null} silently filters the item; the chunk continues without
 * incrementing the skip counter.
 */
@Component
public class DimensionItemProcessor implements ItemProcessor<DimensionRecord, DimensionRecord> {

    private static final Logger log = LoggerFactory.getLogger(DimensionItemProcessor.class);

    private static final int PERSON_ID_MAX   = 100;
    private static final int GRID_ID_MAX     = 100;
    private static final int COUNTRY_MAX     = 10;

    @Override
    public DimensionRecord process(@NonNull DimensionRecord item) {
        // person_id is the merge key — drop rows without it.
        if (isBlank(item.personId())) {
            log.debug("Skipping record — person_id is blank");
            return null;
        }

        return new DimensionRecord(
                truncate(item.gridId(),      GRID_ID_MAX),
                truncate(item.personId(),    PERSON_ID_MAX),
                truncate(item.countryCode(), COUNTRY_MAX),
                item.ecoSectorCode(),
                item.loadedAt());
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() > maxLen ? s.substring(0, maxLen) : s;
    }
}
