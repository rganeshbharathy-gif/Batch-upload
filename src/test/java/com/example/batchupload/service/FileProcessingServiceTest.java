package com.example.batchupload.service;

import com.example.batchupload.model.FileProcessingLog;
import com.example.batchupload.repository.FileProcessingLogRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FileProcessingServiceTest {

    @Mock
    private FileProcessingLogRepository repository;

    @InjectMocks
    private FileProcessingService service;

    @Test
    void isAlreadyClaimed_true() {
        when(repository.existsByS3BucketAndS3KeyAndStatusIn("b", "k", List.of("PROCESSING", "COMPLETED")))
                .thenReturn(true);

        assertThat(service.isAlreadyClaimed("b", "k")).isTrue();
    }

    @Test
    void isAlreadyClaimed_false() {
        when(repository.existsByS3BucketAndS3KeyAndStatusIn("b", "k", List.of("PROCESSING", "COMPLETED")))
                .thenReturn(false);

        assertThat(service.isAlreadyClaimed("b", "k")).isFalse();
    }

    @Test
    void claim_savesAndReturnsId() {
        FileProcessingLog saved = new FileProcessingLog("file.dat", "b", "k", 1024L, "pod-0");
        // Use reflection to set id since it's generated
        try {
            var idField = FileProcessingLog.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(saved, 99L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        when(repository.save(any(FileProcessingLog.class))).thenReturn(saved);

        long id = service.claim("b", "k", "file.dat", 1024L, "pod-0");
        assertThat(id).isEqualTo(99L);
    }

    @Test
    void markCompleted_updatesEntity() {
        FileProcessingLog log = new FileProcessingLog("f.dat", "b", "k", 100L, "pod-0");
        when(repository.findById(1L)).thenReturn(Optional.of(log));
        when(repository.save(any())).thenReturn(log);

        service.markCompleted(1L, 42L, 5000L, 5000L);

        assertThat(log.getStatus()).isEqualTo("COMPLETED");
        assertThat(log.getJobExecutionId()).isEqualTo(42L);
        assertThat(log.getRowCount()).isEqualTo(5000L);
        assertThat(log.getExpectedRowCount()).isEqualTo(5000L);
        verify(repository).save(log);
    }

    @Test
    void markCompleted_throwsIfNotFound() {
        when(repository.findById(999L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.markCompleted(999L, 1L, 100L, 100L))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void markFailed_updatesEntity() {
        FileProcessingLog log = new FileProcessingLog("f.dat", "b", "k", 100L, "pod-0");
        when(repository.findById(1L)).thenReturn(Optional.of(log));
        when(repository.save(any())).thenReturn(log);

        service.markFailed(1L, "error occurred");

        assertThat(log.getStatus()).isEqualTo("FAILED");
        assertThat(log.getErrorMessage()).isEqualTo("error occurred");
        verify(repository).save(log);
    }

    @Test
    void releaseForReprocessing_deletesCompletedAndFailed() {
        service.releaseForReprocessing("b", "k");

        verify(repository).deleteByS3BucketAndS3KeyAndStatusIn(
                eq("b"), eq("k"), eq(List.of("COMPLETED", "FAILED")));
    }
}
