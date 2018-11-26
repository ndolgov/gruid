package org.apache.druid.server.grpc.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.druid.server.grpc.GrpcRowBatch.QueryRequest;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchResponse;
import org.apache.druid.server.grpc.RowBatchQueryServiceGrpc;
import org.apache.druid.server.grpc.RowBatchQueryServiceGrpc.RowBatchQueryServiceStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Create GRPC transport to a given "host:port" destination.<br>
 * For every (request, callback) pair, call the callback on a given executor when a response is received.
 *
 * todo support TLS (http://www.grpc.io/docs/guides/auth.html)
 */
public final class GrpcClient {
    private static final Logger logger = LoggerFactory.getLogger(GrpcClient.class);

    private final ManagedChannel channel;

    private final ExecutorService executor;

    private final RowBatchQueryServiceStub stub;

    public GrpcClient(String host, int port, ExecutorService executor) {
        this.executor = executor;
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
        this.stub = RowBatchQueryServiceGrpc.newStub(channel);
    }

    private RowBatchQueryServiceStub service() {
        return stub;
    }

    public void call(QueryRequest request, StreamObserver<RowBatchResponse> observer) {
        logger.info("Sending request: " + request);
        service().process(request, observer);
    }

    public void close() {
        try {
            logger.info("Closing client");

            executor.shutdown();
            channel.shutdown().awaitTermination(5_000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while shutting down");
        }
    }
}
