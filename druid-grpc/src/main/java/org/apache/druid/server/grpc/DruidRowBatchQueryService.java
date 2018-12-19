package org.apache.druid.server.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.druid.server.grpc.GrpcRowBatch.QueryRequest;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchResponse;
import org.apache.druid.server.grpc.RowBatchQueryServiceGrpc.RowBatchQueryServiceImplBase;

import java.util.concurrent.ExecutorService;

/** Complete gRPC service skeleton by executing a query on a thread from the provided thread pool */
final class DruidRowBatchQueryService extends RowBatchQueryServiceImplBase {
  private final ExecutorService executor;

  private final GrpcQueryExecutor queryExecutor;

  public DruidRowBatchQueryService(ExecutorService executor, GrpcQueryExecutor queryExecutor) {
    this.executor = executor;
    this.queryExecutor = queryExecutor;
  }

  @Override
  public void process(QueryRequest request, StreamObserver<RowBatchResponse> observer) {
    executor.submit(() -> queryExecutor.execute(request.getQuery(), observer));
  }
}
