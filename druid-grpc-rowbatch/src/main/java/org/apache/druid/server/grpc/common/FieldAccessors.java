package org.apache.druid.server.grpc.common;

/**
 * Make possible to adapt the RowBatch mechanism to different input row formats.
 * Each accessor can read a primitive value from a single input row field.
 */
public final class FieldAccessors {
    /**
     * Extract a long value form the current input record
     * @param <T> input record type
     */
    public interface LongFieldAccessor<T> {
        long get(T row);
    }

    /**
     * Extract a long value form the current input record
     * @param <T> input record type
     */
    public interface DoubleFieldAccessor<T> {
        double NULL = RowBatch.NULL_DOUBLE;

        double get(T row);
    }

    /**
     * Extract an int value form the current input record
     * @param <T> input record type
     */
    public interface IntFieldAccessor<T> {
        int get(T row);
    }
}
