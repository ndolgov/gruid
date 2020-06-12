package org.apache.druid.server.grpc.common;

/**
 * A data structure intended to efficiently collect and store micro-batches (e.g. <1K rows) of rows. Every field is
 * represented with an array of the same length.
 *
 * Null values are represented by {@link #NULL_DOUBLE Double.NaN} for doubles and
 * {@link org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder#NULL -1} for longs.
 */
public final class RowBatch {
    public static Double NULL_DOUBLE = Double.NaN;

    final long[][] longColumns;

    final int[][] intColumns;

    final double[][] doubleColumns;

    final int capacity; // field array length

    int index = 0; // the next array offset available for writing; invariant: <=capacity

    public RowBatch(int nLongColumns, int nDoubleColumns, int nIntColumns, int capacity) {
        this.capacity = capacity;
        this.longColumns = new long[nLongColumns][capacity];
        this.doubleColumns = new double[nDoubleColumns][capacity];
        this.intColumns = new int[nIntColumns][capacity];
    }

    public RowBatch(long[][] longColumns, int[][] intColumns, double[][] doubleColumns, int capacity, int size) {
        this.longColumns = longColumns;
        this.intColumns = intColumns;
        this.doubleColumns = doubleColumns;
        this.capacity = capacity;
        this.index = size;
    }

    public boolean isFull() {
        return index >= capacity;
    }

    public RowBatch reset() {
        index = 0;
        return this;
    }

    public void inc() {
        index += 1;
    }

    public boolean hasCapacity(int nLongColumns, int nDoubleColumns, int nIntColumns, int capacity) {
        return (longColumns.length >= nLongColumns) && (doubleColumns.length >= nDoubleColumns) && (intColumns.length >= nIntColumns) && (this.capacity >= capacity);
    }

    public boolean eq(RowBatch that) {
        boolean sameSize =
            index == that.index &&
            longColumns.length == that.longColumns.length &&
            intColumns.length == that.intColumns.length &&
            doubleColumns.length == that.doubleColumns.length;

        if (!sameSize) {
            return false;
        }

        for (int i = 0; i < longColumns.length; i++) {
            final long[] thisColumn = longColumns[i];
            final long[] thatColumn = that.longColumns[i];
            for (int j = 0; j < index; j++) {
                if (thisColumn[j] != thatColumn[j]) {
                    return false;
                }
            }
        }

        for (int i = 0; i < intColumns.length; i++) {
            final int[] thisColumn = intColumns[i];
            final int[] thatColumn = that.intColumns[i];
            for (int j = 0; j < index; j++) {
                if (thisColumn[j] != thatColumn[j]) {
                    return false;
                }
            }
        }

        for (int i = 0; i < doubleColumns.length; i++) {
            final double[] thisColumn = doubleColumns[i];
            final double[] thatColumn = that.doubleColumns[i];

            for (int j = 0; j < index; j++) {
              final double lvalue = thisColumn[j];
              final double rvalue = thatColumn[j];
              if ((lvalue != rvalue) && !(Double.isNaN(lvalue) && Double.isNaN(rvalue))) {
                    return false; // this ain't no sql so NULL == NULL
                }
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return "{RowBatch:capacity=" + capacity + ", LCs=" + longColumns.length + ", ICs=" + intColumns.length + ", DCs=" + doubleColumns.length + "}";
    }
}
