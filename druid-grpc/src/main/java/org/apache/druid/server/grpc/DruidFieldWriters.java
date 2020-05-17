package org.apache.druid.server.grpc;

import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchemaMetric;
import org.apache.druid.server.grpc.common.RowBatch;
import org.apache.druid.server.grpc.common.RowBatchWriters;
import org.apache.druid.server.grpc.common.RowBatchWriters.RowBatchWriter;

/**
 * Map logical fields onto physical ones.
 * Schemas vary in the number of dimension/metric fields only: | T | D1 .. Dn | M1 .. Mm |
 */
public final class DruidFieldWriters
{
  public static RowBatchWriter<ResultRow> timeWriter(RowBatch batch) {
    return RowBatchWriters.longWriter(DruidFieldAccessors.timeAccessor(), batch, 0);
  }
  
  public static RowBatchWriter<ResultRow> dimensionWriter(int index, DictionaryEncoder dictionary, RowBatch batch, int columnIndex) {
    return RowBatchWriters.intWriter(DruidFieldAccessors.dimensionAccessor(index, dictionary), batch, columnIndex);
  }

  public static RowBatchWriter<ResultRow> doubleMetricWriter(int index, RowBatch batch, int columnIndex) {
    return RowBatchWriters.doubleWriter(DruidFieldAccessors.doubleMetricAccessor(index), batch, columnIndex);
  }

  public static RowBatchWriter<ResultRow> longMetricWriter(int index, RowBatch batch, int columnIndex) {
    return RowBatchWriters.longWriter(DruidFieldAccessors.longMetricAccessor(index), batch, columnIndex);
  }

  public static RowBatchWriter<ResultRow> rowWriter(QuerySchema schema, RowBatch batch, DictionaryEncoder dictionary) {
    final int timeDimensionCount = schema.hasTimeDimension ? 1 : 0;
    final RowBatchWriter<ResultRow>[] writers = new RowBatchWriter[timeDimensionCount + schema.dimensions.size() + schema.metrics.size()];
    int writerIndex = 0;

    if (schema.hasTimeDimension) {
      writers[writerIndex++] = timeWriter(batch);
    }

    for (int dimensionIndex = 0; dimensionIndex < schema.dimensions.size(); dimensionIndex++) {
      writers[writerIndex] = dimensionWriter(writerIndex++, dictionary, batch, dimensionIndex);
    }

    for (int metricIndex = 0; metricIndex < schema.metrics.size(); metricIndex++) {
      final QuerySchemaMetric metric = schema.metrics.get(metricIndex);
      switch (metric.type) {
        case DOUBLE:
          writers[writerIndex] = doubleMetricWriter(writerIndex++, batch, metricIndex);
          break;

        case LONG:
          writers[writerIndex] = longMetricWriter(writerIndex++, batch, metricIndex);
          break;

        default:
          throw new IllegalArgumentException("Unexpected metric type: " + metric.type);
      }
    }

    return RowBatchWriters.rowWriter(writers);
  }

}
