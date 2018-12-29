package org.apache.druid.server.grpc.common;

import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema;

public class RowBatchReaders {
  public static RowBatchReader reader(QuerySchema schema) {
    final RowBuffer row = new RowBuffer(new long[schema.dimensions.size() + 1], new double[schema.metrics.size()]);
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
      public long timestamp;
      public final long[] dimensions;
      public final double[] measures;

      public RowBuffer(long[] dimensions, double[] measures) {
          this.dimensions = dimensions;
          this.measures = measures;
      }
  }

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

      for (int i = 1; i < batch.longColumns.length; i++) {
        row.dimensions[i - 1] = batch.longColumns[i][batch.index]; // TODO long metrics
      }

      for (int i = 0; i < batch.doubleColumns.length; i++) {
        row.measures[i] = batch.doubleColumns[i][batch.index];
      }

      batch.inc();

      return row;
    }

    @Override
    public RowBatchReader reset(RowBatch batch) {
      this.batch = batch;
      return this;
    }
  }
}
