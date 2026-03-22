package com.example.batchupload;

import com.example.batchupload.lock.LockExtensionService;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link LockExtensionService} using a mock {@link SimpleLock}.
 * No Spring context or DB required.
 */
class LockExtensionServiceTest {

    /**
     * Verifies that the extension thread calls {@code lock.extend()} at least
     * once within two extension intervals, and that the updated SimpleLock is
     * used for subsequent operations.
     */
    @Test
    void extendsLockPeriodically() throws InterruptedException {
        // Short intervals so the test runs in milliseconds.
        Duration interval  = Duration.ofMillis(100);
        Duration extension = Duration.ofMillis(500);

        LockExtensionService service = new LockExtensionService(extension, interval);

        // First lock: extend() returns a second lock.
        SimpleLock secondLock = mock(SimpleLock.class);
        when(secondLock.extend(any(), any())).thenReturn(Optional.of(secondLock));

        SimpleLock firstLock = mock(SimpleLock.class);
        CountDownLatch extendCalled = new CountDownLatch(1);
        when(firstLock.extend(extension, Duration.ZERO)).thenAnswer(inv -> {
            extendCalled.countDown();
            return Optional.of(secondLock);
        });

        service.start(firstLock);
        boolean extended = extendCalled.await(2, TimeUnit.SECONDS);
        service.stop();
        service.unlockCurrent();

        assertThat(extended).as("extend() should have been called within 2 s").isTrue();
        verify(firstLock, atLeastOnce()).extend(extension, Duration.ZERO);
        // After extension, the service holds secondLock → unlock() on secondLock
        verify(secondLock, atLeastOnce()).unlock();
    }

    /**
     * Verifies that the service logs a warning but continues running when
     * {@code extend()} returns empty (e.g. lock expired due to a long GC pause).
     */
    @Test
    void continuesAfterExtensionFailure() throws InterruptedException {
        Duration interval  = Duration.ofMillis(80);
        Duration extension = Duration.ofMillis(400);

        LockExtensionService service = new LockExtensionService(extension, interval);

        AtomicInteger extendCount = new AtomicInteger();
        SimpleLock lock = mock(SimpleLock.class);
        // First call returns empty (simulated expiry), second returns success.
        SimpleLock renewedLock = mock(SimpleLock.class);
        when(renewedLock.extend(any(), any())).thenReturn(Optional.of(renewedLock));
        when(lock.extend(any(), any()))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(renewedLock));

        service.start(lock);
        Thread.sleep(300); // let two extension attempts fire
        service.stop();
        service.unlockCurrent();

        // Should have attempted at least two extensions
        verify(lock, atLeast(1)).extend(extension, Duration.ZERO);
    }

    /**
     * Verifies that stopping the service immediately interrupts the background
     * thread without waiting for the next extension interval.
     */
    @Test
    void stopInterruptsExtensionThread() throws InterruptedException {
        Duration interval  = Duration.ofMinutes(10); // very long – should not fire
        Duration extension = Duration.ofMinutes(30);

        LockExtensionService service = new LockExtensionService(extension, interval);
        SimpleLock lock = mock(SimpleLock.class);

        long before = System.currentTimeMillis();
        service.start(lock);
        service.stop();           // should return almost immediately
        service.unlockCurrent();
        long elapsed = System.currentTimeMillis() - before;

        // If stop() properly interrupts the virtual thread, elapsed < 1 s.
        assertThat(elapsed).isLessThan(1_000L);
        // extend() must NOT have been called (interval is 10 min)
        verify(lock, never()).extend(any(), any());
        verify(lock).unlock();
    }
}
