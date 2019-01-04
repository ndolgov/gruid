package org.apache.druid.server.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.druid.server.grpc.client.GrpcClient;
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder;
import org.apache.druid.server.grpc.common.Marshallers;
import org.apache.druid.server.grpc.common.RowBatch;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchResponse;
import org.apache.druid.server.grpc.common.RowBatchReaders;
import org.apache.druid.server.grpc.common.ToQuerySchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;
import org.junit.Ignore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class DruidClientTest
{
    private static final Logger logger = LoggerFactory.getLogger(DruidClientTest.class);
    private static final int PORT = 20001;
    private static final long REQUEST_ID = 123;
    private static final String HOSTNAME = "localhost";
    private static final int THREADS = 4;
    private static final String RESULT = "RESULT";

    @Test
    @Ignore // TODO comment out to run when a Druid broker is available
    public void testDruidQueryRequest() throws Exception {
        final GrpcClient client = new GrpcClient(HOSTNAME, PORT, clientExecutor());

        try {
            invokeRpc(client).get(5_000, TimeUnit.MILLISECONDS);
        } finally {
            client.close();
        }
    }

    private CompletableFuture invokeRpc(GrpcClient client) {
        final CompletableFuture future = new CompletableFuture();

        client.call(requestA(), new StreamObserver<RowBatchResponse>() {
            private RowBatchReaders.RowBatchReader reader;

            @Override
            public void onNext(RowBatchResponse response) {
                logger.info("onNext ");

                if (response.hasSchema()) {
                    logger.info("   " + response.getSchema());
                    reader = RowBatchReaders.reader(ToQuerySchema.convert(response.getSchema()));
                }
                logger.info("   " + response.getBatch());
                logger.info("   " + response.getDictionary());

                if (!response.getBatch().isEmpty()) {
                    final RowBatch batch = Marshallers.rowBatchMarshaller().unmarshal(response.getBatch().asReadOnlyByteBuffer());
                    reader.reset(batch);
                    logger.info("   " + batch);
                }

                if (!response.getDictionary().isEmpty()) {
                    final DictionaryEncoder dictionary = Marshallers.dictionaryMarshaller().unmarshal(response.getDictionary().toByteArray());
                    logger.info("   " + dictionary.size());
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

    private static final String groupByDimQuery =
      "{\n"
        + "    \"queryType\": \"groupBy\",\n"
        + "    \"dataSource\": \"foo\",\n"
        + "    \"dimensions\": [\"dim1\", \"dim2\"],\n"
        + "    \"granularity\": \"day\",\n"
        + "    \"intervals\": [\n"
        + "      \"2001-01-01T00:00:00.000/2001-01-05T00:00:00.000\"\n"
        + "    ],\n"
        + "    \"aggregations\": [\n"
        + "      {\n"
        + "        \"type\": \"doubleSum\",\n"
        + "        \"name\": \"M1\",\n"
        + "        \"fieldName\": \"m1\"\n"
        + "      },\n"
        + "      {\n"
        + "        \"type\": \"doubleSum\",\n"
        + "        \"name\": \"M2\",\n"
        + "        \"fieldName\": \"m2\"\n"
        + "      }\n"
        + "    ]\n"
        + "}";

    private static GrpcRowBatch.QueryRequest requestA() {
        return GrpcRowBatch.QueryRequest.newBuilder().setQuery(groupByDimQuery).build();
    }

    private static ExecutorService clientExecutor() {
        return Executors.newFixedThreadPool(THREADS, new ThreadFactoryBuilder().setNameFormat("rpc-client-%d").build());
    }
}
