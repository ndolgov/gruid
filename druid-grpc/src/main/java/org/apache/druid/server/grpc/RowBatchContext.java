package org.apache.druid.server.grpc;

import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema;
import org.apache.druid.server.grpc.common.DictionaryEncoders;
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder;
import org.apache.druid.server.grpc.common.Marshallers;
import org.apache.druid.server.grpc.common.Marshallers.Marshaller;
import org.apache.druid.server.grpc.common.QuerySchemas;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema;
import org.apache.druid.server.grpc.common.RowBatch;
import org.apache.druid.server.grpc.common.RowBatchWriters.RowBatchWriter;
import org.apache.druid.server.grpc.common.ToRowBatchSchema;

import java.nio.ByteBuffer;

final class RowBatchContext
{
  private final Marshaller<RowBatch> batchMarshaller;

  private final Marshaller<DictionaryEncoder> dictionaryMarshaller;

  public final DictionaryEncoder dictionary;

  public final RowBatch batch;

  public final RowBatchSchema schema;

  public final RowBatchWriter<ResultRow> rowWriter;

  public RowBatchContext(int batchSize, QuerySchema schema)
  {
    this.batchMarshaller = Marshallers.rowBatchMarshaller();
    this.dictionary = DictionaryEncoders.dictionaryEncoder();
    this.dictionaryMarshaller = Marshallers.dictionaryMarshaller();
    this.batch = createRowBatch(schema, batchSize);
    this.schema = ToRowBatchSchema.convert(schema);
    this.rowWriter = DruidFieldWriters.rowWriter(schema, batch, dictionary);
  }

  public ByteBuffer marshal(DictionaryEncoder dictionary) {
    return dictionaryMarshaller.marshal(dictionary);
  }

  public ByteBuffer marshal(RowBatch batch) {
    return batchMarshaller.marshal(batch);
  }

  private static RowBatch createRowBatch(QuerySchema schema, int batchSize) {
    final int timeDimensionCount = schema.hasTimeDimension ? 1 : 0;
    final int longMetricCount = (int) schema.metrics.stream().filter(m -> (m.type == QuerySchemas.MetricType.LONG)).count();
    final int doubleMetricCount = (int) schema.metrics.stream().filter(m -> (m.type == QuerySchemas.MetricType.DOUBLE)).count();

    return new RowBatch(timeDimensionCount + longMetricCount, doubleMetricCount, schema.dimensions.size(), batchSize);
  }
}
