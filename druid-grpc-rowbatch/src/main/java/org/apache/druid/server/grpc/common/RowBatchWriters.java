package org.apache.druid.server.grpc.common;

import org.apache.druid.server.grpc.common.FieldAccessors.DoubleFieldAccessor;
import org.apache.druid.server.grpc.common.FieldAccessors.LongFieldAccessor;

/**
 * Assume a row comprised of primitive type values. Support a generic mechanism for extracting values from some input
 * row format and appending them to a RowBatch.
 */
public final class RowBatchWriters {
    /**
     * Append an input record to the batch 
     * @param <T> input record type
     */
    public interface RowBatchWriter<T> {
        void write(T row);
    }

    /**
     * @param writers row field writers
     * @param <T> input record type
     * @return a row writer that can process an entire row
     */
    public static <T> RowBatchWriter<T> rowWriter(RowBatchWriter<T>[] writers) {
        return new RowWriter<>(writers);
    }

    /**
     * @param columnIndex the RowBatch long array to write to; the client is responsible for keeping track of column usage
     */
    public static <T> RowBatchWriter<T> longWriter(LongFieldAccessor<T> accessor, RowBatch batch, int columnIndex) {
        return new LongWriter<>(accessor, batch, columnIndex);
    }

    /**
     * @param columnIndex the RowBatch double array to write to; the client is responsible for keeping track of column usage
     */
    public static <T> RowBatchWriter<T> doubleWriter(DoubleFieldAccessor<T> accessor, RowBatch batch, int columnIndex) {
        return new DoubleWriter<>(accessor, batch, columnIndex);
    }

    static final class LongWriter<T> implements RowBatchWriter<T> {
        private final LongFieldAccessor<T> accessor;
        private final RowBatch batch;
        private final int columnIndex;

        public LongWriter(LongFieldAccessor<T> accessor, RowBatch batch, int columnIndex) {
            this.accessor = accessor;
            this.batch = batch;
            this.columnIndex = columnIndex;
        }

        @Override
        public void write(T row) {
            batch.longColumns[columnIndex][batch.index] = accessor.get(row);
        }
    }

    static final class DoubleWriter<T> implements RowBatchWriter<T> {
        private final DoubleFieldAccessor<T> accessor;
        private final RowBatch batch;
        private final int columnIndex;

        public DoubleWriter(DoubleFieldAccessor<T> accessor, RowBatch batch, int columnIndex) {
            this.accessor = accessor;
            this.batch = batch;
            this.columnIndex = columnIndex;
        }

        @Override
        public void write(T row) {
            batch.doubleColumns[columnIndex][batch.index] = accessor.get(row);
        }
    }

    static final class RowWriter<T> implements RowBatchWriter<T> {
        private final RowBatchWriter<T>[] writers;

        public RowWriter(RowBatchWriter<T>[] writers) {
            this.writers = writers;
        }

        @Override
        public void write(T row) {
            for (RowBatchWriter<T> writer : writers) {
                writer.write(row);
            }
        }
    }
}
