package org.apache.druid.server.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchResponse;

/** Entry point into gRPC-friendly query execution */
public interface GrpcQueryExecutor
{
  /**
   * Execute a query and notify the observer about produced results
   * @param query a query expression
   * @param observer results destination
   */
  void execute(String query, StreamObserver<RowBatchResponse> observer);
}
