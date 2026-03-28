package com.example.batchupload.scheduler;

import com.example.batchupload.repository.PodProcessingLogRepository;
import com.example.batchupload.service.DimensionCleanupService;
import com.example.batchupload.service.FileProcessingService;
import com.example.batchupload.service.S3FileService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.StepExecution;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobSchedulerTest {

    @Mock private JobLauncher jobLauncher;
    @Mock private Job dimensionLoadJob;
    @Mock private FileProcessingService fileProcessingService;
    @Mock private S3FileService s3FileService;
    @Mock private PodProcessingLogRepository podProcessingLogRepository;
    @Mock private DimensionCleanupService dimensionCleanupService;

    private JobScheduler scheduler;

    @BeforeEach
    void setUp() throws Exception {
        scheduler = new JobScheduler(
                jobLauncher, dimensionLoadJob,
                fileProcessingService, s3FileService,
                podProcessingLogRepository, dimensionCleanupService);

        setField("podIndex", 0);
        setField("totalPods", 5);
        setField("s3Bucket", "test-bucket");
        setField("s3Key", "data/dimensions.dat");
    }

    private void setField(String name, Object value) throws Exception {
        Field field = JobScheduler.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(scheduler, value);
    }

    private JobExecution createSuccessfulExecution() {
        JobInstance jobInstance = new JobInstance(1L, "testJob");
        JobExecution execution = new JobExecution(jobInstance, 10L, new JobParameters());
        execution.setStatus(BatchStatus.COMPLETED);

        StepExecution stepExecution = new StepExecution("loadStep", execution);
        stepExecution.setWriteCount(5000);
        stepExecution.setStartTime(LocalDateTime.now().minusMinutes(5));
        stepExecution.setEndTime(LocalDateTime.now());
        execution.addStepExecutions(Set.of(stepExecution));

        return execution;
    }

    @Test
    void runJob_happyPath() throws Exception {
        when(fileProcessingService.isAlreadyClaimed("test-bucket", "data/dimensions.dat")).thenReturn(false);
        when(s3FileService.getFileSize("test-bucket", "data/dimensions.dat")).thenReturn(1_000_000L);
        when(fileProcessingService.claim(eq("test-bucket"), eq("data/dimensions.dat"), anyString(), anyLong(), anyString()))
                .thenReturn(1L);
        when(jobLauncher.run(any(), any())).thenReturn(createSuccessfulExecution());
        when(podProcessingLogRepository.findExpectedRowCountByJobExecutionId(10L)).thenReturn(5000L);

        scheduler.runDimensionLoadJob();

        verify(fileProcessingService).releaseForReprocessing("test-bucket", "data/dimensions.dat");
        verify(dimensionCleanupService).deleteStaleRows();
        verify(fileProcessingService).markCompleted(eq(1L), eq(10L), eq(5000L), eq(5000L));
    }

    @Test
    void runJob_alreadyClaimed_skips() throws Exception {
        when(fileProcessingService.isAlreadyClaimed("test-bucket", "data/dimensions.dat")).thenReturn(true);

        scheduler.runDimensionLoadJob();

        verify(fileProcessingService).releaseForReprocessing("test-bucket", "data/dimensions.dat");
        verify(jobLauncher, never()).run(any(), any());
    }

    @Test
    void runJob_claimThrows_skips() throws Exception {
        when(fileProcessingService.isAlreadyClaimed("test-bucket", "data/dimensions.dat")).thenReturn(false);
        when(s3FileService.getFileSize("test-bucket", "data/dimensions.dat")).thenReturn(1_000_000L);
        when(fileProcessingService.claim(anyString(), anyString(), anyString(), anyLong(), anyString()))
                .thenThrow(new RuntimeException("Duplicate key"));

        scheduler.runDimensionLoadJob();

        verify(jobLauncher, never()).run(any(), any());
    }

    @Test
    void runJob_jobFails_marksFailed() throws Exception {
        when(fileProcessingService.isAlreadyClaimed("test-bucket", "data/dimensions.dat")).thenReturn(false);
        when(s3FileService.getFileSize("test-bucket", "data/dimensions.dat")).thenReturn(1_000_000L);
        when(fileProcessingService.claim(anyString(), anyString(), anyString(), anyLong(), anyString()))
                .thenReturn(1L);
        when(jobLauncher.run(any(), any())).thenThrow(new RuntimeException("Job failed"));

        scheduler.runDimensionLoadJob();

        verify(fileProcessingService).markFailed(eq(1L), eq("Job failed"));
    }

    @Test
    void runJob_callsDeleteStaleRows() throws Exception {
        when(fileProcessingService.isAlreadyClaimed("test-bucket", "data/dimensions.dat")).thenReturn(false);
        when(s3FileService.getFileSize("test-bucket", "data/dimensions.dat")).thenReturn(1_000_000L);
        when(fileProcessingService.claim(anyString(), anyString(), anyString(), anyLong(), anyString()))
                .thenReturn(1L);
        when(jobLauncher.run(any(), any())).thenReturn(createSuccessfulExecution());

        scheduler.runDimensionLoadJob();

        verify(dimensionCleanupService).deleteStaleRows();
    }
}
