package org.apache.druid.server.grpc.common;

import org.apache.druid.server.grpc.common.Marshallers.RowBatchMarshallerImpl;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.apache.druid.server.grpc.common.RowBatch.NULL_DOUBLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class RowBatchTest {
    @Test
    public void testRowBatchMarshaller() {
        final RowBatch batch = createRowBatch();

        final RowBatchMarshallerImpl marshaller = new RowBatchMarshallerImpl();
        final ByteBuffer buffer = marshaller.marshal(batch);
        buffer.rewind();
        final RowBatch unmarshalled = marshaller.unmarshal(buffer);

        assertTrue(batch.eq(unmarshalled));
    }

    public static RowBatch createRowBatch() {
        final RowBatch batch = new RowBatch(2, 3, 2);
        batch.longColumns[0][0] = 2018;
        batch.longColumns[0][1] = 2019;

        batch.longColumns[1][0] = 42;
        batch.longColumns[1][1] = -1;

        batch.doubleColumns[0][0] = 3.14;
        batch.doubleColumns[0][1] = 2.71;

        batch.doubleColumns[1][0] = 9.81;
        batch.doubleColumns[1][1] = 300000;

        batch.doubleColumns[2][0] = NULL_DOUBLE;
        batch.doubleColumns[2][1] = NULL_DOUBLE;

        batch.inc();
        batch.inc();
        return batch;
    }
}
