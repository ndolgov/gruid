package net.ndolgov.druid.forecast.aggregation

import com.google.protobuf.UnsafeByteOperations
import net.ndolgov.druid.forecast.aggregation.Ops.{aggregate, forecast, groupBy, sink, source}
import net.ndolgov.druid.forecast.{BlockingRowBatchIterator, ForecastDimension, ForecastMetric, ForecastPeriod, ForecastRequest, TestDruidServer, TimeUtil, TimeUtils}
import net.ndolgov.druid.forecast.regression.LinearRegressionForecaster
import org.apache.druid.java.util.common.granularity.Granularities
import org.apache.druid.server.grpc.GrpcRowBatch.{RowBatchResponse, RowBatchSchema}
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema.RowBatchField
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema.RowBatchField.RowBatchFieldType
import org.apache.druid.server.grpc.common.{DictionaryEncoders, Marshallers, RowBatch, ToQuerySchema}
import org.apache.druid.server.grpc.common.DictionaryEncoders.DictionaryEncoder
import org.apache.druid.server.grpc.common.RowBatchReaders.RowBuffer
import org.joda.time.{DateTime, Interval}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

class HashGroupSpec extends FlatSpec with GivenWhenThen with Matchers with MockitoSugar {
  def toString(row: RowBuffer, dic: DictionaryEncoder): String =
    s"(${new DateTime(row.timestamp)}|${row.dimensions.map(i => dic.decode(i)).mkString("|")} -> ${row.longMetrics.mkString("|")} | ${row.doubleMetrics.mkString("|")})"

  it should "compose operators into pipeline" in {
    val may22 = "2020-05-22T00:00:00.000Z"
    val may29 = "2020-05-29T00:00:00.000Z"

    val May23 = 1590192000000L
    val timeUtils = TimeUtils(() => May23)
    val request = ForecastRequest(
      "N/A",
      Seq(ForecastDimension("D1")),
      Seq(ForecastMetric("M1", "min")),
      ForecastPeriod(timeUtils.todayInterval(), 5, 60),
      Granularities.DAY
    )

    val forecastInterval = new Interval(DateTime.parse("2020-05-10T00:00:00.000Z"), DateTime.parse("2020-05-24T00:00:00.000Z"))
    //val dailyBuckets = Granularities.DAY.getIterable(forecastInterval).asScala
    val weeklyBuckets = Granularities.WEEK.getIterable(forecastInterval).asScala

    val dictionary = DictionaryEncoders.dictionaryEncoder()
    val batch = HashGroupSpec.testRowBatch(dictionary)
    val batchSchema = HashGroupSpec.schema()
    val batches = new BlockingRowBatchIterator(600000)
    batches.append(RowBatchResponse.newBuilder.setSchema(batchSchema).setBatch(UnsafeByteOperations.unsafeWrap(Marshallers.rowBatchMarshaller.marshal(batch))).build)
    batches.append(RowBatchResponse.newBuilder.setDictionary(UnsafeByteOperations.unsafeWrap(Marshallers.dictionaryMarshaller.marshal(dictionary))).build)

    sink(
      aggregate(
        forecast(
          groupBy(
            source(ToQuerySchema.convert(batchSchema), batches),
            2, 7, 3),
          new LinearRegressionForecaster(), DateTime.parse("2020-05-23T00:00:00.000Z"), 3),
        Seq(AggregateFunctions("min"), AggregateFunctions("max")),
        weeklyBuckets)).
    exec (null)
  }

  it should "group by country" in {
    val dictionary = DictionaryEncoders.dictionaryEncoder()
    val groups = HashGroupSpec(dictionary)

    groups.size shouldEqual 2
    dictionary.size shouldEqual 2 + 1

    val groupList = groups.iterator().toList
    val american = groupList.find(hg => hg.dimensions(0) == dictionary.encode("US")).get
    american.dimensions(1) shouldEqual dictionary.encode("iphone")
    american.measures(0).slice(0, 7) shouldEqual Array(100, 102, 104, 106, 108, 110, 112)
    american.measures(1).slice(0, 7) shouldEqual Array(1, 2, 3, 4, 5, 6, 7)
    val canadian = groupList.find(hg => hg.dimensions(0) == dictionary.encode("CAN")).get
    canadian.dimensions(1) shouldEqual dictionary.encode("iphone")
    canadian.measures(0).slice(0, 7) shouldEqual Array(200, 205, 210, 215, 220, 225, 230)
    canadian.measures(1).slice(0, 7) shouldEqual Array(1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7)

  }

  it should "Generate forecast day timestamps" in {
    val timestamps = ForecastPeriods.forecastDayTimestamps(DateTime.parse("2020-05-25T00:00:00.000Z"), 3)

    timestamps.toArray shouldEqual Array(
      DateTime.parse("2020-05-25T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-26T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-27T00:00:00.000Z").getMillis)
  }
}

// "today" is assumed to be May 28
object HashGroupSpec {
  val Country = "country"
  val Cost = "cost"
  val Weight = "weight"
  val Product = "product"
  val Time = "time"

  def schema(): RowBatchSchema =
    RowBatchSchema.newBuilder
      .addFields(RowBatchField.newBuilder.setFieldName(Time).setFieldType(RowBatchFieldType.TIME))
      .addFields(RowBatchField.newBuilder.setFieldName(Country).setFieldType(RowBatchFieldType.DIMENSION))
      .addFields(RowBatchField.newBuilder.setFieldName(Product).setFieldType(RowBatchFieldType.DIMENSION))
      .addFields(RowBatchField.newBuilder.setFieldName(Cost).setFieldType(RowBatchFieldType.DOUBLE_METRIC))
      .addFields(RowBatchField.newBuilder.setFieldName(Weight).setFieldType(RowBatchFieldType.DOUBLE_METRIC))
      .build

  def testRowBatch(dictionary: DictionaryEncoder = DictionaryEncoders.dictionaryEncoder()): RowBatch = {
    val Capacity = Rows.length

    val intColumns = Array.ofDim[Int](2, Capacity)
    val doubleColumns = Array.ofDim[Double](2, Capacity)
    val longColumns = Array.ofDim[Long](1, Capacity)

    var i = 0
    Rows.foreach(row => {
      longColumns(0)(i) = DateTime.parse(row(Time).take(10)).getMillis

      intColumns(0)(i) = dictionary.encode(row(Country))
      intColumns(1)(i) = dictionary.encode(row(Product))

      doubleColumns(0)(i) = row(Cost).toDouble
      doubleColumns(1)(i) = row(Weight).toDouble

      i += 1
    })

    new RowBatch(longColumns, intColumns, doubleColumns, Capacity, Capacity)
  }

  val Rows: Array[Map[String, String]] = Array(
    Map(Time -> "2020-05-11T00:00:00.000Z", Cost -> "100", Weight -> "1", Country -> "US", Product -> "iphone"),
    Map(Time -> "2020-05-11T00:00:00.000Z", Cost -> "200", Weight -> "1.1", Country -> "CAN", Product -> "iphone"),
    Map(Time -> "2020-05-13T00:00:00.000Z", Cost -> "102", Weight -> "2", Country -> "US", Product -> "iphone"),
    Map(Time -> "2020-05-13T00:00:00.000Z", Cost -> "205", Weight -> "1.2", Country -> "CAN", Product -> "iphone"),
    Map(Time -> "2020-05-15T00:00:00.000Z", Cost -> "104", Weight -> "3", Country -> "US", Product -> "iphone"),
    Map(Time -> "2020-05-15T00:00:00.000Z", Cost -> "210", Weight -> "1.3", Country -> "CAN", Product -> "iphone"),
    Map(Time -> "2020-05-17T00:00:00.000Z", Cost -> "106", Weight -> "4", Country -> "US", Product -> "iphone"),
    Map(Time -> "2020-05-17T00:00:00.000Z", Cost -> "215", Weight -> "1.4", Country -> "CAN", Product -> "iphone"),
    Map(Time -> "2020-05-19T00:00:00.000Z", Cost -> "108", Weight -> "5", Country -> "US", Product -> "iphone"),
    Map(Time -> "2020-05-19T00:00:00.000Z", Cost -> "220", Weight -> "1.5", Country -> "CAN", Product -> "iphone"),
    Map(Time -> "2020-05-21T00:00:00.000Z", Cost -> "110", Weight -> "6", Country -> "US", Product -> "iphone"),
    Map(Time -> "2020-05-21T00:00:00.000Z", Cost -> "225", Weight -> "1.6", Country -> "CAN", Product -> "iphone"),
    Map(Time -> "2020-05-23T00:00:00.000Z", Cost -> "112", Weight -> "7", Country -> "US", Product -> "iphone"),
    Map(Time -> "2020-05-23T00:00:00.000Z", Cost -> "230", Weight -> "1.7", Country -> "CAN", Product -> "iphone")
  )

  def apply(dictionary: DictionaryEncoder = DictionaryEncoders.dictionaryEncoder()): HashGroups = {
    val buffer = new RowBuffer(Array(-1, -1), Array(-1, -1), Array.empty)

    val groups = HashGroups(2, 7, 3)

    Rows.foreach(row => {
      buffer.timestamp = TimeUtil.dateToLong(row(Time).take(10))
      buffer.dimensions(0) = dictionary.encode(row(Country))
      buffer.dimensions(1) = dictionary.encode(row(Product))
      buffer.doubleMetrics(0) = row(Cost).toDouble
      buffer.doubleMetrics(1) = row(Weight).toDouble
      groups.get(buffer.dimensions).append(buffer)
    })

    groups
  }
}

