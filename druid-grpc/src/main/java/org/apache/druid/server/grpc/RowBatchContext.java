package org.apache.druid.server.grpc;

import org.apache.druid.data.input.Row;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema;
import org.apache.druid.server.grpc.common.DictionaryEncoders;
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder;
import org.apache.druid.server.grpc.common.Marshallers;
import org.apache.druid.server.grpc.common.Marshallers.Marshaller;
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

  public final RowBatchWriter<Row> rowWriter;

  public RowBatchContext(int batchSize, QuerySchema schema)
  {
    this.batchMarshaller = Marshallers.rowBatchMarshaller();
    this.dictionary = DictionaryEncoders.dictionaryEncoder();
    this.dictionaryMarshaller = Marshallers.dictionaryMarshaller();
    this.batch = new RowBatch(1 + schema.dimensions.size(), schema.metrics.size(), batchSize);
    this.schema = ToRowBatchSchema.convert(schema);
    this.rowWriter = DruidFieldWriters.rowWriter(schema, batch, dictionary);
  }

  public ByteBuffer marshal(DictionaryEncoder dictionary) {
    return dictionaryMarshaller.marshal(dictionary);
  }

  public ByteBuffer marshal(RowBatch batch) {
    return batchMarshaller.marshal(batch);
  }
}
