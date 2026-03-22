package com.example.batchupload.lock;

import net.javacrumbs.shedlock.core.SimpleLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Periodically extends an active ShedLock before it expires, preventing other
 * nodes from stealing the lock while a long-running batch job is still in
 * progress.
 *
 * <h2>Why this is necessary</h2>
 * ShedLock requires a {@code lockAtMostFor} duration at acquisition time.
 * This is a <em>safety ceiling</em>: if the owning node crashes, the lock is
 * automatically released after this period so other nodes can proceed.
 * For a 20 GB file upload the actual runtime is unknown in advance – it could
 * be 30 minutes or 3 hours depending on hardware and DB load.
 * Setting a huge {@code lockAtMostFor} (e.g. 24 h) as a fixed value is
 * dangerous: a crashed node would block all others for a full day.
 *
 * <h2>Solution: rolling extension</h2>
 * <pre>
 *  acquire lock (lockAtMostFor = extensionDuration)
 *  start background virtual thread
 *    every extensionInterval:
 *      lock.extend(extensionDuration) → new SimpleLock
 *  job finishes → stop extension thread → unlock
 * </pre>
 * The lock is always {@code extensionDuration} ahead of the current time.
 * If the node crashes, the lock expires in at most {@code extensionDuration}.
 *
 * <h2>Thread safety</h2>
 * {@link SimpleLock#extend(Duration, Duration)} returns a <em>new</em>
 * {@link SimpleLock} instance. The current lock reference is stored in an
 * {@link AtomicReference} so both the extension thread and the caller can
 * safely read and swap it.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * Optional<SimpleLock> lock = lockProvider.lock(config);
 * if (lock.isEmpty()) { return; }  // another node is running
 *
 * lockExtensionService.start(lock.get());
 * try {
 *     runBatchJob();
 * } finally {
 *     lockExtensionService.stop();
 *     lockExtensionService.unlockCurrent();
 * }
 * }</pre>
 */
@Service
public class LockExtensionService {

    private static final Logger log = LoggerFactory.getLogger(LockExtensionService.class);

    /**
     * How long each extension pushes the {@code lock_until} timestamp forward.
     * Must be greater than {@code extensionInterval} to ensure the lock never
     * expires between two consecutive extension calls.
     * Configurable via {@code shedlock.extension-duration} (default: 30 min).
     */
    private final Duration extensionDuration;

    /**
     * How frequently the background thread wakes up to extend the lock.
     * Should be materially less than {@code extensionDuration} (e.g. half)
     * so a missed extension due to a slow DB round-trip doesn't expire the lock.
     * Configurable via {@code shedlock.extension-interval} (default: 10 min).
     */
    private final Duration extensionInterval;

    /** The currently active lock; updated atomically after each successful extension. */
    private final AtomicReference<SimpleLock> activeLock = new AtomicReference<>();

    /** The background virtual thread running the extension loop. */
    private volatile Thread extensionThread;

    /** Signals the extension loop to stop. */
    private volatile boolean running;

    public LockExtensionService(
            @Value("${shedlock.extension-duration:PT30M}") Duration extensionDuration,
            @Value("${shedlock.extension-interval:PT10M}") Duration extensionInterval) {
        this.extensionDuration = extensionDuration;
        this.extensionInterval = extensionInterval;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Stores the initial lock and starts a background virtual thread that
     * extends it every {@code extensionInterval}.
     *
     * <p>Must be called <strong>before</strong> the batch job starts, while
     * the initial lock is still valid.
     *
     * @param initialLock the {@link SimpleLock} returned by
     *                    {@link net.javacrumbs.shedlock.core.LockProvider#lock}
     */
    public void start(SimpleLock initialLock) {
        activeLock.set(initialLock);
        running = true;

        extensionThread = Thread.ofVirtual()
                .name("shedlock-extender")
                .start(this::extensionLoop);

        log.info("Lock extension started – interval={}, extensionDuration={}",
                extensionInterval, extensionDuration);
    }

    /**
     * Signals the extension loop to stop and interrupts the background thread.
     * Call this in a {@code finally} block after the batch job finishes.
     */
    public void stop() {
        running = false;
        Thread t = extensionThread;
        if (t != null) {
            t.interrupt();
        }
        log.info("Lock extension stopped");
    }

    /**
     * Releases the currently held lock.
     * Always call this after {@link #stop()} to free the DB row immediately
     * rather than waiting for {@code lock_until} to expire naturally.
     */
    public void unlockCurrent() {
        SimpleLock lock = activeLock.getAndSet(null);
        if (lock != null) {
            try {
                lock.unlock();
                log.info("ShedLock released");
            } catch (Exception e) {
                // Unlock failures are logged but not rethrown – the DB row will
                // expire on its own via lock_until, and re-throwing here would
                // shadow the real job outcome exception.
                log.warn("Failed to release ShedLock (will expire automatically): {}", e.getMessage());
            }
        }
    }

    // ── Extension loop ────────────────────────────────────────────────────────

    private void extensionLoop() {
        while (running) {
            try {
                Thread.sleep(extensionInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("Extension thread interrupted – stopping");
                return;
            }

            if (!running) return; // stop() was called during sleep

            extendOnce();
        }
    }

    private void extendOnce() {
        SimpleLock current = activeLock.get();
        if (current == null) {
            log.warn("extendOnce called but activeLock is null – extension skipped");
            return;
        }

        try {
            // extend() pushes lock_until to (now + extensionDuration).
            // It returns a NEW SimpleLock that must be used for the next
            // extend() / unlock() call.
            Optional<SimpleLock> extended = current.extend(
                    extensionDuration,
                    Duration.ZERO   // lockAtLeastFor = 0 → can unlock immediately
            );

            if (extended.isPresent()) {
                activeLock.set(extended.get()); // swap in the refreshed lock
                log.info("ShedLock extended – lock_until pushed forward by {}", extensionDuration);
            } else {
                // Another node may have taken the lock (e.g. after a long GC pause
                // caused lock_until to lapse). Log loudly but don't throw – the
                // batch job may still complete successfully before any other node
                // starts, and stopping here would leave the job half-finished.
                log.error("ShedLock extension FAILED – lock may have expired. "
                        + "Increase shedlock.extension-interval or check for long GC pauses.");
            }

        } catch (Exception e) {
            // DB connectivity issue during extension. Log and keep trying on the
            // next interval rather than terminating the job prematurely.
            log.error("Exception during ShedLock extension – will retry in {}: {}",
                    extensionInterval, e.getMessage(), e);
        }
    }
}
