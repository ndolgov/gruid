package org.apache.druid.server.grpc.common;

import gnu.trove.map.TObjectIntMap;
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder;
import org.apache.druid.server.grpc.common.DictionaryEncoders.TroveDictionaryEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

/** Custom marshallers to/from binary formats. */
public final class Marshallers {
    /**
     * Entity marshaller into/from binary formats
     * @param <T> the entity type
     */
    public interface Marshaller<T> {
        ByteBuffer marshal(T obj);

        T unmarshal(ByteBuffer buffer);

        default T unmarshal(byte[] array) {
            return unmarshal(ByteBuffer.wrap(array));
        }
    }

    public static Marshaller<RowBatch> rowBatchMarshaller() {
        return new RowBatchMarshallerImpl();
    }

    public static Marshaller<DictionaryEncoder> dictionaryMarshaller() {
        return new DictionaryEncoderMarshallerImpl();
    }

    // | flags: Long | size: Int | keyi: String | valuei: Int |
    static final class DictionaryEncoderMarshallerImpl implements Marshaller<DictionaryEncoder>
    {
        @Override
        public ByteBuffer marshal(DictionaryEncoder obj) {
            final TroveDictionaryEncoder dictionary = (TroveDictionaryEncoder) obj;

            final ByteBufferOutputStream baos = new ByteBufferOutputStream(estimatedSizeInBytes(dictionary.size()));
            final DataOutputStream dos = new DataOutputStream(baos);
            try {
                dos.writeLong(1L);
                dos.writeInt(dictionary.size());

                dictionary.traverse((name, code) -> {
                    try {
                        dos.writeUTF(name);
                        dos.writeInt(code);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to marshal entry: " + name + "=" + code, e);
                    }
                    return true;
                });
                dos.flush();

                return baos.toByteBuffer();
            } catch (Exception e) {
                throw new RuntimeException("Failed to marshal dictionary of size: " + dictionary.size(), e);
            }
        }

        @Override
        public DictionaryEncoder unmarshal(ByteBuffer buffer) {
            final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer.array()));

            try {
                final long flags = dis.readLong();

                final int count = dis.readInt();

                final TObjectIntMap<String> strToInt = TroveDictionaryEncoder.createMap(count);
                for (int i = 0; i < count; i++) {
                    strToInt.put(dis.readUTF(), dis.readInt());
                }

                return new TroveDictionaryEncoder(strToInt);
            } catch (Exception e) {
                throw new RuntimeException("Failed to unmarshal dictionary from buffer of size: " + buffer.capacity(), e);
            }
        }

        private int estimatedSizeInBytes(int size) {
            return Long.BYTES + Integer.BYTES + (Integer.BYTES + 20)*size;
        }

    }

    // | flags: Long | nLongCols: Int | nDoubleCols: Int | colLength: Int |  LongCol1 | ... | DoubleColN  |
    static final class RowBatchMarshallerImpl implements Marshaller<RowBatch>
    {
        @Override
        public ByteBuffer marshal(RowBatch batch) {
            int colLength = batch.index;
            int nLongCols = batch.longColumns.length;
            int nDoubleCols = batch.doubleColumns.length;

            final int requiredSize = requiredSizeInBytes(colLength, nLongCols, nDoubleCols);
            final ByteBuffer buffer = ByteBuffer.allocate(requiredSize);

            buffer.putLong(1);

            buffer.putInt(nLongCols);
            buffer.putInt(nDoubleCols);
            buffer.putInt(colLength);

            for (long[] column : batch.longColumns) {
                for (int i = 0; i < colLength; i++) {
                    buffer.putLong(column[i]);
                }
            }

            for (double[] column : batch.doubleColumns) {
                for (int i = 0; i < colLength; i++) {
                    buffer.putDouble(column[i]);
                }
            }

            buffer.flip();
            return buffer;
        }

        private int requiredSizeInBytes(int colLength, int nLongCols, int nDoubleCols) {
            return Long.BYTES + Integer.BYTES*3 + nLongCols*colLength*Long.BYTES + nDoubleCols*colLength*Double.BYTES;
        }

        @Override
        public RowBatch unmarshal(ByteBuffer buffer) {
            final long flags = buffer.getLong();

            final int nLongCols = buffer.getInt();
            final int nDoubleCols = buffer.getInt();
            final int colLength = buffer.getInt();

            //final int requiredSize = requiredSizeInBytes(colLength, nLongCols, nDoubleCols);
            return readColumns(buffer, new RowBatch(nLongCols, nDoubleCols, colLength), colLength);
        }

        public RowBatch unmarshal(ByteBuffer buffer, RowBatch batch) {
            final long flags = buffer.getLong();

            final int nLongCols = buffer.getInt();
            final int nDoubleCols = buffer.getInt();
            final int colLength = buffer.getInt();

            final RowBatch unmarshalled;
            if (batch.hasCapacity(nLongCols, nDoubleCols, colLength)) {
                unmarshalled = readColumns(buffer, batch, colLength);
            } else {
                unmarshalled = readColumns(buffer, new RowBatch(nLongCols, nDoubleCols, colLength), colLength);
            }

            return unmarshalled;
        }

        private RowBatch readColumns(ByteBuffer buffer, RowBatch batch, int colLength) {
            for (long[] column : batch.longColumns) {
                for (int i = 0; i < colLength; i++) {
                    column[i] = buffer.getLong();
                }
            }

            for (double[] column : batch.doubleColumns) {
                for (int i = 0; i < colLength; i++) {
                    column[i] = buffer.getDouble();
                }
            }

            batch.index = colLength;

            return batch;
        }
    }

    private static final class ByteBufferOutputStream extends ByteArrayOutputStream {
        public ByteBufferOutputStream(int size) {
            super(size);
        }

        public ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf, 0, count);
        }
    }
}
