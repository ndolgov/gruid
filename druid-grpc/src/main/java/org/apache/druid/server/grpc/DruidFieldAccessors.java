package org.apache.druid.server.grpc;

import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder;
import org.apache.druid.server.grpc.common.FieldAccessors.DoubleFieldAccessor;
import org.apache.druid.server.grpc.common.FieldAccessors.LongFieldAccessor;

/** Extract field values from a ResultRow returned by Druid query execution (i.e. a QueryLifecycle). */
public final class DruidFieldAccessors
{
  public static LongFieldAccessor<ResultRow> timeAccessor()
  {
    return new TimeAccessor();
  }

  public static LongFieldAccessor<ResultRow> dimensionAccessor(int index, DictionaryEncoder dictionary)
  {
    return new DimensionAccessor(index, dictionary);
  }

  public static DoubleFieldAccessor<ResultRow> doubleMetricAccessor(int index)
  {
    return new DoubleMetricAccessor(index);
  }

  static final class TimeAccessor implements LongFieldAccessor<ResultRow>
  {
    @Override
    public long get(ResultRow row)
    {
      return row.getLong(0);
    }
  }

  static final class DimensionAccessor implements LongFieldAccessor<ResultRow>
  {
    private final DictionaryEncoder dictionary;
    private final int index;

    public DimensionAccessor(int index, DictionaryEncoder dictionary)
    {
      this.dictionary = dictionary;
      this.index = index;
    }

    @Override
    public long get(ResultRow row)
    {
      final Object value = row.get(index);
      return (value == null) ? DictionaryEncoder.NULL : dictionary.encode(value.toString());
    }
  }

  static final class DoubleMetricAccessor implements DoubleFieldAccessor<ResultRow>
  {
    private final int index;

    public DoubleMetricAccessor(int index)
    {
      this.index = index;
    }

    @Override
    public double get(ResultRow row)
    {
      final Number value = (Number) row.get(index);
      return (value == null) ? NULL : value.doubleValue();
    }
  }

  private DruidFieldAccessors() {
  }
}
