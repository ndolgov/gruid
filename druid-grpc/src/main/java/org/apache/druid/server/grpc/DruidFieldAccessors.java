package org.apache.druid.server.grpc;

import org.apache.druid.data.input.Row;
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder;
import org.apache.druid.server.grpc.common.FieldAccessors.DoubleFieldAccessor;
import org.apache.druid.server.grpc.common.FieldAccessors.LongFieldAccessor;

/** Extract field values from a Row returned by Druid query execution (i.e. a QueryLifecycle). */
public final class DruidFieldAccessors
{
  public static LongFieldAccessor<Row> timeAccessor()
  {
    return new TimeAccessor();
  }

  public static LongFieldAccessor<Row> dimensionAccessor(String dimensionName, DictionaryEncoder dictionary)
  {
    return new DimensionAccessor(dimensionName, dictionary);
  }

  public static DoubleFieldAccessor<Row> doubleMetricAccessor(String metricName)
  {
    return new DoubleMetricAccessor(metricName);
  }

  static final class TimeAccessor implements LongFieldAccessor<Row>
  {
    @Override
    public long get(Row row)
    {
      return row.getTimestamp().getMillis();
    }
  }

  static final class DimensionAccessor implements LongFieldAccessor<Row>
  {
    private final DictionaryEncoder dictionary;
    private final String dimensionName;

    public DimensionAccessor(String dimensionName, DictionaryEncoder dictionary)
    {
      this.dictionary = dictionary;
      this.dimensionName = dimensionName;
    }

    @Override
    public long get(Row row)
    {
      final Object value = row.getRaw(dimensionName);
      return (value == null) ? DictionaryEncoder.NULL : dictionary.encode(value.toString());
    }
  }

  static final class DoubleMetricAccessor implements DoubleFieldAccessor<Row>
  {
    private final String metricName;

    public DoubleMetricAccessor(String metricName)
    {
      this.metricName = metricName;
    }

    @Override
    public double get(Row row)
    {
      final Number value = row.getMetric(metricName);
      return (value == null) ? NULL : value.doubleValue();
    }
  }
}
