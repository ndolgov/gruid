package org.apache.druid.server.grpc;

import org.apache.druid.data.input.Row;
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
  public static RowBatchWriter<Row> timeWriter(RowBatch batch) {
    return RowBatchWriters.longWriter(DruidFieldAccessors.timeAccessor(), batch, 0);
  }
  
  public static RowBatchWriter<Row> dimensionWriter(String dimensionName, DictionaryEncoder dictionary, RowBatch batch, int columnIndex) {
    return RowBatchWriters.longWriter(DruidFieldAccessors.dimensionAccessor(dimensionName, dictionary), batch, columnIndex);
  }

  public static RowBatchWriter<Row> doubleMetricWriter(String metricName, RowBatch batch, int columnIndex) {
    return RowBatchWriters.doubleWriter(DruidFieldAccessors.doubleMetricAccessor(metricName), batch, columnIndex);
  }

  public static RowBatchWriter<Row> rowWriter(QuerySchema schema, RowBatch batch, DictionaryEncoder dictionary) {
    final RowBatchWriter<Row>[] writers = new RowBatchWriter[1 + schema.dimensions.size() + schema.metrics.size()]; 
    int writerIndex = 0;
    
    writers[writerIndex++] = timeWriter(batch);

    for (int dimensionIndex = 0; dimensionIndex < schema.dimensions.size(); dimensionIndex++) {
      writers[writerIndex++] = dimensionWriter(schema.dimensions.get(dimensionIndex), dictionary, batch, dimensionIndex);
    }

    for (int metricIndex = 0; metricIndex < schema.metrics.size(); metricIndex++) {
      final QuerySchemaMetric metric = schema.metrics.get(metricIndex);
      switch (metric.type) {
        case DOUBLE:
          writers[writerIndex++] = doubleMetricWriter(metric.name, batch, metricIndex);
          break;

        default:
          throw new IllegalArgumentException("Unexpected metric type: " + metric.type); // todo support long metrics?
      }
    }

    return RowBatchWriters.rowWriter(writers);
  }

}
