package org.apache.druid.server.grpc.common;

import org.apache.druid.server.grpc.GrpcRowBatch;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema.RowBatchField;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema.RowBatchField.RowBatchFieldType;
import org.apache.druid.server.grpc.common.QuerySchemas.MetricType;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema;

/** Generate the protobuf version of a QuerySchema */
public final class ToRowBatchSchema
{
  public static final String TIME = "Time"; // todo dedicated QuerySchema type?

  public static RowBatchSchema convert(QuerySchema querySchema) {
    final GrpcRowBatch.RowBatchSchema.Builder builder = GrpcRowBatch.RowBatchSchema.newBuilder()
      .addFields(field(TIME, RowBatchFieldType.TIME));

      querySchema.dimensions.forEach(dimensionName -> builder.addFields(field(dimensionName, RowBatchFieldType.DIMENSION)));

      querySchema.metrics.forEach(metric -> builder.addFields(field(metric.name, metricType(metric.type))));

      return builder.build();
  }

  private static RowBatchFieldType metricType(MetricType metricType) {
    switch (metricType) {
      case LONG: return RowBatchFieldType.LONG_METRIC;

      case DOUBLE: return RowBatchFieldType.DOUBLE_METRIC;

      default: throw new IllegalArgumentException("Unexpected metric type: " + metricType);
    }
  }

  private static RowBatchField.Builder field(String name, RowBatchFieldType type) {
    return RowBatchField.newBuilder().
      setFieldName(name).
      setFieldType(type);
  }
}
