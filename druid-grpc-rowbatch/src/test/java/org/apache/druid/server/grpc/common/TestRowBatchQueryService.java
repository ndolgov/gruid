package org.apache.druid.server.grpc.common;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import org.apache.druid.server.grpc.GrpcRowBatch.QueryRequest;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchResponse;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema;
import org.apache.druid.server.grpc.RowBatchQueryServiceGrpc.RowBatchQueryServiceImplBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

final class TestRowBatchQueryService extends RowBatchQueryServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(TestRowBatchQueryService.class);

    private final ExecutorService executor;
    private final RowBatchSchema schema;
    private final ByteBuffer batch;
    private final ByteBuffer dictionary;

    public TestRowBatchQueryService(ExecutorService executor, RowBatchSchema schema, ByteBuffer batch, ByteBuffer dictionary) {
        this.executor = executor;
        this.schema = schema;
        this.batch = batch;
        this.dictionary = dictionary;
    }

    @Override
    public void process(QueryRequest request, StreamObserver<RowBatchResponse> observer) {
        executor.submit(() -> {
            logger.info("Processing query: " + request.getQuery());
            if (request.getQuery().isEmpty()) {
                observer.onError(new IllegalArgumentException("Empty query"));
            }

            logger.info("Sending schema");
            observer.onNext(RowBatchResponse.newBuilder().setSchema(schema).build());

            logger.info("Sending rows");
            observer.onNext(RowBatchResponse.newBuilder().setBatch(UnsafeByteOperations.unsafeWrap(batch)).build());

            logger.info("Sending dictionary");
            observer.onNext(RowBatchResponse.newBuilder().setDictionary(UnsafeByteOperations.unsafeWrap(dictionary)).build());

            logger.info("Done");
            observer.onCompleted();
        });
    }
}
