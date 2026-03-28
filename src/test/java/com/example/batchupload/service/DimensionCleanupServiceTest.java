package com.example.batchupload.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DimensionCleanupServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private DimensionCleanupService service;

    @Test
    void deleteStaleRows_executesCorrectSql() {
        when(jdbcTemplate.update(eq("DELETE FROM DIMENSIONS WHERE LOAD_DATE < TRUNC(SYSDATE)"))).thenReturn(100);

        service.deleteStaleRows();

        verify(jdbcTemplate).update("DELETE FROM DIMENSIONS WHERE LOAD_DATE < TRUNC(SYSDATE)");
    }

    @Test
    void deleteStaleRows_returnsDeletedCount() {
        when(jdbcTemplate.update(eq("DELETE FROM DIMENSIONS WHERE LOAD_DATE < TRUNC(SYSDATE)"))).thenReturn(42);

        int result = service.deleteStaleRows();

        assertThat(result).isEqualTo(42);
    }
}
