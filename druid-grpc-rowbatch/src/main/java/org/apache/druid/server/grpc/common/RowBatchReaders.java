package org.apache.druid.server.grpc.common;

import org.apache.druid.server.grpc.common.QuerySchemas.MetricType;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema;

public class RowBatchReaders {
  public static RowBatchReader reader(QuerySchema schema) {
    final int longMetricCount = (int) schema.metrics.stream().filter(m -> (m.type == MetricType.LONG)).count();
    final int doubleMetricCount = (int) schema.metrics.stream().filter(m -> (m.type == MetricType.DOUBLE)).count();

    final RowBuffer row = new RowBuffer(new int[schema.dimensions.size()], new double[doubleMetricCount], new long[longMetricCount]);
    return new RowBatchReaderImpl(row);
  }

  /**
   * A row-at-a-time RowBatch iterator
   */
  public interface RowBatchReader {
    boolean hasNext();

    /**
     * Read next available row from the underlying row buffer
     * @return the next row as a buffer that will be reused for reading the next row
     */
    RowBuffer next();

    /**
     * @param batch the next batch to iterate
     * @return this reader reset to read from the new batch
     */
    RowBatchReader reset(RowBatch batch);
  }

  /** A single row of data to be used as a mutable buffer while traversing a large dataset */
  public static final class RowBuffer {
      public long timestamp; // the usual Unix time in milliseconds
      public final int[] dimensions; // dictionary-encoded
      public final double[] doubleMetrics;
      public final long[] longMetrics;

      public RowBuffer(int[] dimensions, double[] doubleMetrics, long[] longMetrics) {
        this.dimensions = dimensions;
        this.doubleMetrics = doubleMetrics;
        this.longMetrics = longMetrics;
      }
  }

  // assume timestamp column is always present
  private static final class RowBatchReaderImpl implements RowBatchReader {
    private final RowBuffer row;
    private RowBatch batch;

    private RowBatchReaderImpl(RowBuffer row) {
      this.row = row;
    }

    @Override
    public boolean hasNext() {
      return !batch.isFull();
    }

    @Override
    public RowBuffer next() {
      row.timestamp = batch.longColumns[0][batch.index];

      for (int i = 1; i < batch.longColumns.length; i++) { // adjust index for time in the 1st long column
        row.longMetrics[i - 1] = batch.longColumns[i][batch.index];
      }

      for (int i = 0; i < batch.doubleColumns.length; i++) {
        row.doubleMetrics[i] = batch.doubleColumns[i][batch.index];
      }

      for (int i = 0; i < batch.intColumns.length; i++) {
        row.dimensions[i] = batch.intColumns[i][batch.index];
      }

      batch.inc();

      return row;
    }

    @Override
    public RowBatchReader reset(RowBatch batch) {
      this.batch = batch.reset();
      return this;
    }
  }
}
