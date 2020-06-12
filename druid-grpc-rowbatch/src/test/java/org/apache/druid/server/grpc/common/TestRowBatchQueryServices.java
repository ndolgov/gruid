package org.apache.druid.server.grpc.common;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.BindableService;
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema;
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestRowBatchQueryServices {
    private static final int THREADS = 4;

    public static List<BindableService> bindableService(RowBatchSchema schema, DictionaryEncoder dictionary, RowBatch batch) {
        final ByteBuffer rowBatch = Marshallers.rowBatchMarshaller().marshal(batch);

        return Lists.newArrayList(
            new TestRowBatchQueryService(
                executor("handler-%d"),
                schema,
                rowBatch,
                Marshallers.dictionaryMarshaller().marshal(dictionary)));
    }

    public static ExecutorService executor(String nameFormat) {
        return Executors.newFixedThreadPool(THREADS, new ThreadFactoryBuilder().setNameFormat(nameFormat).build());
    }
}
