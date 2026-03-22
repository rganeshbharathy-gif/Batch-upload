package com.example.batchupload.processor;

import com.example.batchupload.model.DimensionRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * Validates and sanitises each {@link DimensionRecord} before it reaches the writer.
 *
 * <p>Returning {@code null} causes Spring Batch to silently skip the item — no exception,
 * no retry, no rollback. Use this for business-rule filtering (e.g. blank mandatory fields).
 */
@Slf4j
@Component
public class DimensionItemProcessor implements ItemProcessor<DimensionRecord, DimensionRecord> {

    @Override
    public DimensionRecord process(@NonNull DimensionRecord item) {
        // Drop records where the two primary identifiers are both blank
        if (isBlank(item.getCsiId()) && isBlank(item.getPersonId())) {
            log.debug("Skipping record — both csi_id and person_id are blank");
            return null;
        }

        // Truncate to Oracle column limits to avoid ORA-12899
        item.setCsiId(truncate(item.getCsiId(), 100));
        item.setPersonId(truncate(item.getPersonId(), 100));
        item.setCountryCode(truncate(item.getCountryCode(), 10));
        item.setEconomicCode(truncate(item.getEconomicCode(), 50));

        return item;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() > maxLen ? s.substring(0, maxLen) : s;
    }
}
