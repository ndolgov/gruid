package org.apache.druid.server.grpc.common;

import org.apache.druid.server.grpc.common.Marshallers.RowBatchMarshallerImpl;
import org.apache.druid.server.grpc.common.RowBatchReaders.RowBuffer;
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
        true,
          Lists.newArrayList(
            new QuerySchemas.QuerySchemaDimension(1, "D1")),
          Lists.newArrayList(
            new QuerySchemas.QuerySchemaMetric(2, "M1", QuerySchemas.MetricType.LONG),
            new QuerySchemas.QuerySchemaMetric(3, "M2", QuerySchemas.MetricType.DOUBLE),
            new QuerySchemas.QuerySchemaMetric(4, "M3", QuerySchemas.MetricType.DOUBLE),
            new QuerySchemas.QuerySchemaMetric(5, "M4", QuerySchemas.MetricType.DOUBLE)));

        final RowBatchReaders.RowBatchReader reader = RowBatchReaders.reader(schema).reset(createRowBatch());

        assertTrue(reader.hasNext());
        final RowBuffer row1 = reader.next();
        assertEquals(row1.timestamp, 2018);
        assertEquals(row1.dimensions[0], 2020);
        assertEquals(row1.longMetrics[0], 42);
        assertEquals(row1.doubleMetrics[0], 3.14, 0.000001);

        assertTrue(reader.hasNext());
        final RowBuffer row2 = reader.next();
        assertEquals(row2.timestamp, 2019);
        assertEquals(row2.dimensions[0], 43);
        assertEquals(row2.longMetrics[0], -1);
        assertEquals(row2.doubleMetrics[0], 2.71, 0.000001);

        assertFalse(reader.hasNext());
    }

    public static RowBatch createRowBatch() {
        final RowBatch batch = new RowBatch(2, 3, 1,2);
        // T
        batch.longColumns[0][0] = 2018;
        batch.longColumns[0][1] = 2019;

        // M1
        batch.longColumns[1][0] = 42;
        batch.longColumns[1][1] = -1;

        // M2
        batch.doubleColumns[0][0] = 3.14;
        batch.doubleColumns[0][1] = 2.71;

        // M3
        batch.doubleColumns[1][0] = 9.81;
        batch.doubleColumns[1][1] = 300000;

        // M4
        batch.doubleColumns[2][0] = NULL_DOUBLE;
        batch.doubleColumns[2][1] = NULL_DOUBLE;

        // D1
        batch.intColumns[0][0] = 2020;
        batch.intColumns[0][1] = 43;

        batch.inc();
        batch.inc();
        return batch;
    }
}
