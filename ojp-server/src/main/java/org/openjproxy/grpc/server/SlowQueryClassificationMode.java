package org.openjproxy.grpc.server;

/**
 * Classification strategies for Slow Query Segregation.
 */
public enum SlowQueryClassificationMode {
    RELATIVE_AVERAGE,
    ABSOLUTE_THRESHOLD
}
