package org.apache.druid.server.grpc.common;

import org.apache.druid.server.grpc.common.Marshallers.RowBatchMarshallerImpl;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.nio.ByteBuffer;

import static org.apache.druid.server.grpc.common.RowBatch.NULL_DOUBLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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

    @Test
    public void testRowBatchReader() {
        final QuerySchemas.QuerySchema schema = new QuerySchemas.QuerySchema(
          Lists.newArrayList("D1"),
          Lists.newArrayList(
            new QuerySchemas.QuerySchemaMetric("M1", QuerySchemas.MetricType.DOUBLE),
            new QuerySchemas.QuerySchemaMetric("M2", QuerySchemas.MetricType.DOUBLE),
            new QuerySchemas.QuerySchemaMetric("M3", QuerySchemas.MetricType.DOUBLE)));

        final RowBatch batch = createRowBatch();
        batch.reset();

        final RowBatchReaders.RowBatchReader reader = RowBatchReaders.reader(schema).reset(batch);

        assertTrue(reader.hasNext());
        assertEquals(reader.next().timestamp, 2018);
        assertEquals(reader.next().timestamp, 2019);
        assertFalse(reader.hasNext());
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
