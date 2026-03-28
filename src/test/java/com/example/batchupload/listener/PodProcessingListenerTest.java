package com.example.batchupload.listener;

import com.example.batchupload.model.PodProcessingLog;
import com.example.batchupload.repository.PodProcessingLogRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.step.Step;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PodProcessingListenerTest {

    @Mock
    private PodProcessingLogRepository repository;

    private PodProcessingListener createListener() {
        return new PodProcessingListener(repository, 2, 5, "bucket", "key/file.dat", 1000L, 5000L);
    }

    private StepExecution createStepExecution() {
        JobInstance jobInstance = new JobInstance(1L, "testJob");
        JobExecution jobExecution = new JobExecution(jobInstance, 100L, new JobParameters());
        StepExecution stepExecution = new StepExecution("loadStep", jobExecution);
        stepExecution.setStatus(BatchStatus.COMPLETED);
        stepExecution.setStartTime(LocalDateTime.now().minusMinutes(5));
        stepExecution.setEndTime(LocalDateTime.now());
        return stepExecution;
    }

    @Test
    void afterStep_savesLogWithCorrectFields() {
        PodProcessingListener listener = createListener();
        StepExecution stepExecution = createStepExecution();
        stepExecution.setReadCount(200);
        stepExecution.setWriteCount(195);
        stepExecution.setReadSkipCount(3);
        stepExecution.setWriteSkipCount(1);
        stepExecution.setProcessSkipCount(1);

        listener.afterStep(stepExecution);

        ArgumentCaptor<PodProcessingLog> captor = ArgumentCaptor.forClass(PodProcessingLog.class);
        verify(repository).save(captor.capture());

        PodProcessingLog saved = captor.getValue();
        assertThat(saved.getJobExecutionId()).isEqualTo(100L);
        assertThat(saved.getPodIndex()).isEqualTo(2);
        assertThat(saved.getTotalPods()).isEqualTo(5);
        assertThat(saved.getS3Bucket()).isEqualTo("bucket");
        assertThat(saved.getS3Key()).isEqualTo("key/file.dat");
        assertThat(saved.getStartByte()).isEqualTo(1000L);
        assertThat(saved.getEndByte()).isEqualTo(5000L);
        assertThat(saved.getReadCount()).isEqualTo(200L);
        assertThat(saved.getWriteCount()).isEqualTo(195L);
        assertThat(saved.getSkipCount()).isEqualTo(5L);
        assertThat(saved.getStatus()).isEqualTo("COMPLETED");
        assertThat(saved.getExpectedRowCount()).isNull();
    }

    @Test
    void afterStep_readsExpectedRowCountFromContext() {
        PodProcessingListener listener = createListener();
        StepExecution stepExecution = createStepExecution();
        stepExecution.getExecutionContext().putLong("footer.expectedRowCount", 50000L);

        listener.afterStep(stepExecution);

        ArgumentCaptor<PodProcessingLog> captor = ArgumentCaptor.forClass(PodProcessingLog.class);
        verify(repository).save(captor.capture());
        assertThat(captor.getValue().getExpectedRowCount()).isEqualTo(50000L);
    }

    @Test
    void afterStep_nullExpectedRowCountWhenMissing() {
        PodProcessingListener listener = createListener();
        StepExecution stepExecution = createStepExecution();
        // Do not put footer.expectedRowCount in context

        listener.afterStep(stepExecution);

        ArgumentCaptor<PodProcessingLog> captor = ArgumentCaptor.forClass(PodProcessingLog.class);
        verify(repository).save(captor.capture());
        assertThat(captor.getValue().getExpectedRowCount()).isNull();
    }
}
