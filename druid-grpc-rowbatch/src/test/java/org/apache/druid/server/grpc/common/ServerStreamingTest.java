package org.apache.druid.server.grpc.common;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import org.apache.druid.server.grpc.GrpcRowBatch;
import org.apache.druid.server.grpc.GrpcRowBatch.QueryRequest;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchResponse;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema.RowBatchField;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema.RowBatchField.RowBatchFieldType;
import org.apache.druid.server.grpc.client.GrpcClient;
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder;
import org.apache.druid.server.grpc.server.GrpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.druid.server.grpc.common.TestRowBatchQueryServices.executor;
import static org.testng.Assert.*;

public final class ServerStreamingTest {
    private static final Logger logger = LoggerFactory.getLogger(ServerStreamingTest.class);
    private static final int PORT = 20000;
    private static final String HOSTNAME = "127.0.0.1";

    @Test
    public void testRowBatchStreaming() throws Exception {
        final GrpcServer server = new GrpcServer(HOSTNAME, PORT, services(), executor("rpc-server-%d"));
        server.start();


        final GrpcClient client = new GrpcClient(HOSTNAME, PORT, executor("rpc-client-%d"));

        try {
            invokeRpc(client).get(5_000, TimeUnit.MILLISECONDS);
        } finally {
            client.close();

            server.stop();
        }
    }

    private static List<BindableService> services() {
        RowBatchField.newBuilder().setFieldName("").setFieldType(RowBatchFieldType.TIME);
        final GrpcRowBatch.RowBatchSchema schema = GrpcRowBatch.RowBatchSchema.newBuilder().
            addFields(RowBatchField.newBuilder().setFieldName("Time").setFieldType(RowBatchFieldType.TIME)).
            addFields(RowBatchField.newBuilder().setFieldName("D1").setFieldType(RowBatchFieldType.DIMENSION)).
            addFields(RowBatchField.newBuilder().setFieldName("M1").setFieldType(RowBatchFieldType.DOUBLE_METRIC)).
            addFields(RowBatchField.newBuilder().setFieldName("M2").setFieldType(RowBatchFieldType.DOUBLE_METRIC)).
            build();

        final DictionaryEncoder dictionary = DictionaryEncoders.dictionaryEncoder();
        dictionary.encode("US");
        dictionary.encode("CAN");

        final RowBatch originalBatch = RowBatchTest.createRowBatch();
        return TestRowBatchQueryServices.bindableService(schema, dictionary, originalBatch);
    }

    private CompletableFuture invokeRpc(GrpcClient client) {
        final CompletableFuture future = new CompletableFuture();

        client.call(QueryRequest.newBuilder().setQuery("todo").build(), new StreamObserver<RowBatchResponse>() {
            @Override
            public void onNext(RowBatchResponse response) {
                logger.info("onNext ");

                if (response.hasSchema()) {
                    logger.info("   " + response.getSchema());
                    assertEquals(response.getSchema().getFieldsCount(), 4);
                }
                logger.info("   " + response.getBatch());
                logger.info("   " + response.getDictionary());

                if (!response.getBatch().isEmpty()) {
                    final RowBatch batch = Marshallers.rowBatchMarshaller().unmarshal(response.getBatch().asReadOnlyByteBuffer());
                    assertEquals(batch.capacity, 2);
                }

                if (!response.getDictionary().isEmpty()) {
                    final DictionaryEncoder dictionary = Marshallers.dictionaryMarshaller().unmarshal(response.getDictionary().toByteArray());
                    assertEquals(dictionary.size(), 2);
                }
            }

            @Override
            public void onError(Throwable e) {
                logger.error("onError", e);
                future.completeExceptionally(e);
            }

            @Override
            public void onCompleted() {
                logger.info("onCompleted");
                future.complete(null);
            }
        });

        return future;
    }
}
