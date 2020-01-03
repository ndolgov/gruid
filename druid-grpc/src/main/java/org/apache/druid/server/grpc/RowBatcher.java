package org.apache.druid.server.grpc;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchResponse;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema;
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder;

public final class RowBatcher {
  private static final Logger log = new Logger(RowBatcher.class);

  private final StreamObserver<RowBatchResponse> observer;

  private final RowBatchContext ctx;

  private State state = State.FIRST_ROW; // todo GoF?

  public RowBatcher(StreamObserver<RowBatchResponse> observer, RowBatchContext ctx)
  {
    this.observer = observer;
//    this.dictionary = dictionary;
//    this.dictionaryMarshaller = dictionaryMarshaller;
//    this.batch = new RowBatch(1 + schema.dimensions.size(), schema.metrics.size(), batchSize);
//    this.schema = ToRowBatchSchema.convert(schema);
//    this.batchMarshaller = batchMarshaller;
//    this.rowWriter = DruidFieldWriters.rowWriter(schema, batch, dictionary);
    this.ctx = ctx;
  }

  public void onRow(ResultRow row) {
    log.info("Processing: " + row);

    if (ctx.batch.isFull()) {
      flush(state == State.FIRST_ROW ? ctx.schema : null, null);
    }

    ctx.rowWriter.write(row);
    ctx.batch.inc();
  }

  public void onError(Exception e) {
    log.info("Reporting: " + e.getMessage());
    observer.onError(e);
    state = State.COMPLETED;
  }

  public void onComplete() {
    if (state == State.COMPLETED) {
      throw new IllegalStateException("Already completed");
    }

    flush(state == State.FIRST_ROW ? ctx.schema : null, ctx.dictionary);
    observer.onCompleted();

    state = State.COMPLETED;
  }

  private void flush(RowBatchSchema schema, DictionaryEncoder dictionary)
  {
    final RowBatchResponse.Builder builder = RowBatchResponse.newBuilder();

    if (schema != null) {
      builder.setSchema(schema);
    }

    if (dictionary != null) {
      builder.setDictionary(UnsafeByteOperations.unsafeWrap(ctx.marshal(dictionary)));
    }

    builder.setBatch(UnsafeByteOperations.unsafeWrap(ctx.marshal(ctx.batch)));
    ctx.batch.reset();

    observer.onNext(builder.build());

    state = State.AFTER_FIRST_ROW;
  }

  private enum State {FIRST_ROW, AFTER_FIRST_ROW, COMPLETED}
}
