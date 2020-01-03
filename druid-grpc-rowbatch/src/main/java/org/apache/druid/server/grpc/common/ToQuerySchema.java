package org.apache.druid.server.grpc.common;

import com.google.common.collect.Lists;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema.RowBatchField;
import org.apache.druid.server.grpc.common.QuerySchemas.MetricType;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchemaDimension;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchemaMetric;

import java.util.List;

/** Generate the QuerySchema version of a protobuf schema */
public final class ToQuerySchema {
  public static QuerySchema convert(RowBatchSchema batchSchema) {
    final List<QuerySchemaDimension> dimensions = Lists.newArrayList();
    final List<QuerySchemaMetric> metrics = Lists.newArrayList();
    boolean hasTimeDimension = false;
    int index = 0;

    for (RowBatchField field : batchSchema.getFieldsList()) {
      switch (field.getFieldType()) {
        case TIME:
          hasTimeDimension = true;
          index++;
          break;

        case DIMENSION:
          dimensions.add(new QuerySchemaDimension(index++, field.getFieldName()));
          break;

        case LONG_METRIC:
          metrics.add(new QuerySchemaMetric(index++, field.getFieldName(), MetricType.LONG));
          index++;
          break;

        case DOUBLE_METRIC:
          metrics.add(new QuerySchemaMetric(index++, field.getFieldName(), MetricType.DOUBLE));
          index++;
          break;

        default: throw new IllegalArgumentException("Unexpected field type" + field.getFieldType());
      }
    }

    return new QuerySchema(hasTimeDimension, dimensions, metrics);
  }
}
