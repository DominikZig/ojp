package org.openjproxy.grpc.server;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Monitors the performance of SQL operations and tracks their average execution times.
 *
 * This class tracks execution times for unique operations (identified by their SQL hash)
 * and maintains a rolling average using the formula:
 * new_average = ((stored_average * 4) + new_measurement) / 5
 *
 * This gives 20% weight to the newest measurement, smoothing out outliers.
 *
 * The global average update is configurable and can be controlled by an interval to improve performance.
 * Thread safety is intentionally not implemented for performance reasons - this class prioritizes
 * speed over perfect consistency in a concurrent environment.
 */
@Slf4j
public class QueryPerformanceMonitor {
    public static final long DEFAULT_SLOW_QUERY_THRESHOLD_MS = 1000L;
    public static final SlowQueryClassificationMode DEFAULT_CLASSIFICATION_MODE = SlowQueryClassificationMode.RELATIVE_AVERAGE;

    /**
     * Record for tracking operation performance metrics.
     */
    private static class PerformanceRecord {
        private volatile double averageExecutionTime;
        private final AtomicLong executionCount;
        private final ReentrantLock lock = new ReentrantLock();

        public PerformanceRecord(double initialTime) {
            this.averageExecutionTime = initialTime;
            this.executionCount = new AtomicLong(1);
        }

        /**
         * Updates the average execution time using the weighted formula.
         * new_average = ((stored_average * 4) + new_measurement) / 5
         */
        public void updateAverage(double newMeasurement) {
            lock.lock();
            try {
                this.averageExecutionTime = ((this.averageExecutionTime * 4) + newMeasurement) / 5;
                this.executionCount.incrementAndGet();
            } finally {
                lock.unlock();
            }
        }

        public double getAverageExecutionTime() {
            return averageExecutionTime;
        }

        public long getExecutionCount() {
            return executionCount.get();
        }
    }

    private final ConcurrentHashMap<String, PerformanceRecord> operationRecords = new ConcurrentHashMap<>();
    private volatile double overallAverageExecutionTime = 0.0;
    private final AtomicLong totalOperations = new AtomicLong(0);

    // Global average update interval configuration
    private final long updateGlobalAvgIntervalSeconds;
    private final SlowQueryClassificationMode classificationMode;
    private final long slowQueryThresholdMs;
    private final TimeProvider timeProvider;
    private volatile long lastGlobalAvgUpdateTime = 0L;
    private volatile int lastKnownUniqueQueryCount = 0;

    /**
     * Creates a QueryPerformanceMonitor with default settings (always update global average).
     */
    public QueryPerformanceMonitor() {
        this(0L, TimeProvider.SYSTEM, DEFAULT_CLASSIFICATION_MODE, DEFAULT_SLOW_QUERY_THRESHOLD_MS);
    }

    /**
     * Creates a QueryPerformanceMonitor with specified update interval.
     *
     * @param updateGlobalAvgIntervalSeconds interval in seconds between global average updates.
     *                                      If 0, global average is updated on every query (default behavior).
     */
    public QueryPerformanceMonitor(long updateGlobalAvgIntervalSeconds) {
        this(updateGlobalAvgIntervalSeconds, TimeProvider.SYSTEM,
                DEFAULT_CLASSIFICATION_MODE, DEFAULT_SLOW_QUERY_THRESHOLD_MS);
    }

    /**
     * Creates a QueryPerformanceMonitor with update interval and classification settings.
     *
     * @param updateGlobalAvgIntervalSeconds interval in seconds between global average updates.
     * @param classificationMode classification mode for slow operation detection.
     * @param slowQueryThresholdMs deterministic threshold in milliseconds for ABSOLUTE_THRESHOLD mode.
     */
    public QueryPerformanceMonitor(long updateGlobalAvgIntervalSeconds,
                                   SlowQueryClassificationMode classificationMode,
                                   long slowQueryThresholdMs) {
        this(updateGlobalAvgIntervalSeconds, TimeProvider.SYSTEM, classificationMode, slowQueryThresholdMs);
    }

    /**
     * Creates a QueryPerformanceMonitor with specified update interval and time provider (for testing).
     *
     * @param updateGlobalAvgIntervalSeconds interval in seconds between global average updates.
     *                                      If 0, global average is updated on every query (default behavior).
     * @param timeProvider provider for current time (allows mocking in tests)
     */
    public QueryPerformanceMonitor(long updateGlobalAvgIntervalSeconds, TimeProvider timeProvider) {
        this(updateGlobalAvgIntervalSeconds, timeProvider, DEFAULT_CLASSIFICATION_MODE, DEFAULT_SLOW_QUERY_THRESHOLD_MS);
    }

    /**
     * Creates a QueryPerformanceMonitor with specified settings and time provider (for testing).
     *
     * @param updateGlobalAvgIntervalSeconds interval in seconds between global average updates.
     * @param timeProvider provider for current time (allows mocking in tests)
     * @param classificationMode classification mode for slow operation detection.
     * @param slowQueryThresholdMs deterministic threshold in milliseconds for ABSOLUTE_THRESHOLD mode.
     */
    public QueryPerformanceMonitor(long updateGlobalAvgIntervalSeconds, TimeProvider timeProvider,
                                   SlowQueryClassificationMode classificationMode, long slowQueryThresholdMs) {
        this.updateGlobalAvgIntervalSeconds = updateGlobalAvgIntervalSeconds;
        this.timeProvider = timeProvider;
        this.classificationMode = classificationMode != null
                ? classificationMode
                : DEFAULT_CLASSIFICATION_MODE;
        if (slowQueryThresholdMs < 0) {
            log.warn("Invalid negative slowQueryThresholdMs={}, using default {}ms",
                    slowQueryThresholdMs, DEFAULT_SLOW_QUERY_THRESHOLD_MS);
            this.slowQueryThresholdMs = DEFAULT_SLOW_QUERY_THRESHOLD_MS;
        } else {
            this.slowQueryThresholdMs = slowQueryThresholdMs;
        }
        this.lastGlobalAvgUpdateTime = timeProvider.currentTimeSeconds();
    }

    /**
     * Records the execution time for an operation.
     *
     * @param operationHash The hash of the SQL operation (from SqlStatementXXHash)
     * @param executionTimeMs The execution time in milliseconds
     */
    public void recordExecutionTime(String operationHash, double executionTimeMs) {
        if (operationHash == null || executionTimeMs < 0) {
            log.warn("Invalid operation hash or execution time: hash={}, time={}", operationHash, executionTimeMs);
            return;
        }

        int previousSize = operationRecords.size();

        PerformanceRecord record = operationRecords.compute(operationHash, (key, existing) -> {
            if (existing == null) {
                return new PerformanceRecord(executionTimeMs);
            } else {
                existing.updateAverage(executionTimeMs);
                return existing;
            }
        });

        boolean isNewOperation = operationRecords.size() > previousSize;

        totalOperations.incrementAndGet();

        // Update global average based on interval and conditions
        if (shouldUpdateGlobalAverage(isNewOperation)) {
            updateOverallAverage();
        }

        log.debug("Updated operation {} with execution time {}ms, average now {}ms",
                 operationHash, executionTimeMs, record.getAverageExecutionTime());
    }

    /**
     * Determines if the global average should be updated based on interval and new unique queries.
     *
     * @param isNewOperation true if this is a new unique operation
     * @return true if global average should be updated
     */
    private boolean shouldUpdateGlobalAverage(boolean isNewOperation) {
        // If interval is 0, always update (default behavior)
        if (updateGlobalAvgIntervalSeconds == 0) {
            return true;
        }

        // If this is a new unique operation, always update immediately
        if (isNewOperation) {
            log.debug("Updating global average immediately due to new unique operation");
            return true;
        }

        // Check if enough time has passed since last update
        long currentTime = timeProvider.currentTimeSeconds();
        boolean intervalElapsed = (currentTime - lastGlobalAvgUpdateTime) >= updateGlobalAvgIntervalSeconds;

        if (intervalElapsed) {
            log.debug("Updating global average due to interval elapsed: {} seconds since last update",
                     currentTime - lastGlobalAvgUpdateTime);
            return true;
        }

        return false;
    }

    /**
     * Gets the average execution time for a specific operation.
     *
     * @param operationHash The hash of the SQL operation
     * @return The average execution time in milliseconds, or 0.0 if not found
     */
    public double getOperationAverageTime(String operationHash) {
        PerformanceRecord record = operationRecords.get(operationHash);
        return record != null ? record.getAverageExecutionTime() : 0.0;
    }

    /**
     * Gets the overall average execution time across all tracked operations.
     * This is the average of all individual operation averages.
     *
     * @return The overall average execution time in milliseconds
     */
    public double getOverallAverageExecutionTime() {
        return overallAverageExecutionTime;
    }

    /**
     * Determines if an operation is classified as "slow".
     * RELATIVE_AVERAGE mode: operation average execution time is 2x or greater than the overall average.
     * ABSOLUTE_THRESHOLD mode: operation average execution time is greater than or equal to the configured threshold.
     *
     * @param operationHash The hash of the SQL operation
     * @return true if the operation is classified as slow, false otherwise
     */
    public boolean isSlowOperation(String operationHash) {
        PerformanceRecord record = operationRecords.get(operationHash);
        if (record == null) {
            return false;
        }

        double operationAverage = record.getAverageExecutionTime();
        boolean isSlow;
        if (classificationMode == SlowQueryClassificationMode.ABSOLUTE_THRESHOLD) {
            isSlow = operationAverage >= slowQueryThresholdMs;
            log.debug("Operation {} classification: mode={}, average={}ms, threshold={}ms, slow={}",
                    operationHash, classificationMode, operationAverage, slowQueryThresholdMs, isSlow);
            return isSlow;
        }

        double overallAverage = getOverallAverageExecutionTime();
        // If overall average is 0 or very small, consider all operations as fast initially
        if (overallAverage <= 1.0) {
            return false;
        }
        isSlow = operationAverage >= (overallAverage * 2.0);
        log.debug("Operation {} classification: mode={}, average={}ms, overall={}ms, slow={}",
                operationHash, classificationMode, operationAverage, overallAverage, isSlow);
        return isSlow;
    }

    /**
     * Updates the overall average execution time.
     * This is calculated as the average of all current operation averages.
     * This method is intentionally not synchronized for performance reasons.
     */
    private void updateOverallAverage() {
        if (operationRecords.isEmpty()) {
            overallAverageExecutionTime = 0.0;
            return;
        }

        double sum = operationRecords.values().stream()
                .mapToDouble(PerformanceRecord::getAverageExecutionTime)
                .sum();

        overallAverageExecutionTime = sum / operationRecords.size();

        // Update the last update time and known unique query count
        lastGlobalAvgUpdateTime = timeProvider.currentTimeSeconds();
        lastKnownUniqueQueryCount = operationRecords.size();

        log.trace("Updated overall average execution time to {}ms across {} operations",
                 overallAverageExecutionTime, operationRecords.size());
    }

    /**
     * Gets the number of unique operations being tracked.
     *
     * @return The number of unique operations
     */
    public int getTrackedOperationCount() {
        return operationRecords.size();
    }

    /**
     * Gets the total number of operation executions recorded.
     *
     * @return The total execution count across all operations
     */
    public long getTotalExecutionCount() {
        return totalOperations.get();
    }

    public SlowQueryClassificationMode getClassificationMode() {
        return classificationMode;
    }

    public long getSlowQueryThresholdMs() {
        return slowQueryThresholdMs;
    }

    /**
     * Clears all performance records. Used primarily for testing.
     */
    public void clear() {
        operationRecords.clear();
        overallAverageExecutionTime = 0.0;
        totalOperations.set(0);
        lastGlobalAvgUpdateTime = timeProvider.currentTimeSeconds();
        lastKnownUniqueQueryCount = 0;
        log.info("Performance monitor cleared");
    }
}
