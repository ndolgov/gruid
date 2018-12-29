package org.apache.druid.server.grpc.common;

import com.google.common.collect.Lists;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema.RowBatchField;
import org.apache.druid.server.grpc.common.QuerySchemas.MetricType;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchemaMetric;

import java.util.List;

/** Generate the QuerySchema version of a protobuf schema */
public final class ToQuerySchema {
  public static QuerySchema convert(RowBatchSchema batchSchema) {
    final List<String> dimensions = Lists.newArrayList();
    final List<QuerySchemaMetric > metrics = Lists.newArrayList();

    for (RowBatchField field : batchSchema.getFieldsList()) {
      switch (field.getFieldType()) {
        case TIME: break;

        case DIMENSION:
          dimensions.add(field.getFieldName());
          break;

        case LONG_METRIC:
          metrics.add(new QuerySchemaMetric(field.getFieldName(), MetricType.LONG));
          break;

        case DOUBLE_METRIC:
          metrics.add(new QuerySchemaMetric(field.getFieldName(), MetricType.DOUBLE));
          break;

        default: throw new IllegalArgumentException("Unexpected field type" + field.getFieldType());
      }
    }

      return new QuerySchema(dimensions, metrics);
  }
}
