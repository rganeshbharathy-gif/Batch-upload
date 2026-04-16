package com.example.batchupload.writer;

import com.example.batchupload.model.DimensionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

/**
 * Merges {@link DimensionRecord} chunks into the DIMENSIONS table via a single
 * JDBC batch per chunk. The merge is keyed on {@code person_id} so the daily
 * file upserts the dimension without producing duplicates.
 *
 * <p>Oracle-specific notes
 * <ul>
 *   <li>The {@code MERGE} is compiled once and reused per chunk via the implicit
 *       statement cache (see {@code oracle.jdbc.implicitStatementCacheSize} in
 *       {@code application.yml}).</li>
 *   <li>{@code eco_sector_code} binds as {@code NUMBER}; Oracle accepts a Java
 *       {@code Long} or a {@code null} directly.</li>
 *   <li>{@code loaded_at} is written on every merge (insert <em>and</em> update)
 *       so the timestamp always reflects the most recent job execution.</li>
 * </ul>
 */
@Component
public class DimensionItemWriter implements ItemWriter<DimensionRecord> {

    private static final Logger log = LoggerFactory.getLogger(DimensionItemWriter.class);

    private static final String MERGE_SQL = """
            MERGE INTO dimensions tgt
            USING (SELECT
                       :personId       AS person_id,
                       :gridId         AS grid_id,
                       :countryCode    AS country_code,
                       :ecoSectorCode  AS eco_sector_code,
                       :loadedAt       AS loaded_at
                   FROM DUAL) src
               ON (tgt.person_id = src.person_id)
            WHEN MATCHED THEN UPDATE SET
                   tgt.grid_id         = src.grid_id,
                   tgt.country_code    = src.country_code,
                   tgt.eco_sector_code = src.eco_sector_code,
                   tgt.loaded_at       = src.loaded_at
            WHEN NOT MATCHED THEN INSERT
                   (person_id, grid_id, country_code, eco_sector_code, loaded_at)
            VALUES (src.person_id, src.grid_id, src.country_code, src.eco_sector_code, src.loaded_at)
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public DimensionItemWriter(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void write(Chunk<? extends DimensionRecord> chunk) {
        List<? extends DimensionRecord> items = chunk.getItems();
        if (items.isEmpty()) return;

        SqlParameterSource[] batch = items.stream()
                .map(DimensionItemWriter::toParams)
                .toArray(SqlParameterSource[]::new);

        int[] counts = jdbcTemplate.batchUpdate(MERGE_SQL, batch);
        if (log.isDebugEnabled()) {
            log.debug("Merged {} rows", sum(counts));
        }
    }

    private static MapSqlParameterSource toParams(DimensionRecord r) {
        return new MapSqlParameterSource()
                .addValue("personId",      r.personId())
                .addValue("gridId",        r.gridId())
                .addValue("countryCode",   r.countryCode())
                .addValue("ecoSectorCode", r.ecoSectorCode())
                .addValue("loadedAt",      Timestamp.from(r.loadedAt()));
    }

    private static int sum(int[] arr) {
        int total = 0;
        for (int v : arr) total += v;
        return total;
    }
}
