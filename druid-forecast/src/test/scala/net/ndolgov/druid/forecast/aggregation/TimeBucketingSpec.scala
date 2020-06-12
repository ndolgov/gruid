package net.ndolgov.druid.forecast.aggregation

import org.apache.druid.java.util.common.granularity.Granularities
import org.joda.time.{DateTime, Interval}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

// 2020-05-11 was Monday
class TimeBucketingSpec extends FlatSpec with GivenWhenThen with Matchers with MockitoSugar {
  it should "Generate weekly buckets Mon-next Mon" in {
    val interval = new Interval(DateTime.parse("2020-05-11T00:00:00.000Z"), DateTime.parse("2020-05-18T00:00:00.000Z"))
    val weeklyBuckets = Granularities.WEEK.getIterable(interval).asScala.map(b => b.getEndMillis)

    weeklyBuckets.toArray shouldEqual Array(
      DateTime.parse("2020-05-18T00:00:00.000Z").getMillis
    )
  }

  it should "Generate weekly buckets Wed-next Wed" in {
    val interval = new Interval(DateTime.parse("2020-05-13T00:00:00.000Z"), DateTime.parse("2020-05-20T00:00:00.000Z"))
    val weeklyBuckets = Granularities.WEEK.getIterable(interval).asScala.map(b => b.getEndMillis)

    weeklyBuckets.toArray shouldEqual Array(
      DateTime.parse("2020-05-18T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-25T00:00:00.000Z").getMillis
    )
  }

  it should "Generate weekly buckets Mon-Tue in two weeks" in {
    val interval = new Interval(DateTime.parse("2020-05-11T00:00:00.000Z"), DateTime.parse("2020-05-26T00:00:00.000Z"))
    val weeklyBuckets = Granularities.WEEK.getIterable(interval).asScala.map(b => b.getEndMillis)

    weeklyBuckets.toArray shouldEqual Array(
      DateTime.parse("2020-05-18T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-25T00:00:00.000Z").getMillis,
      DateTime.parse("2020-06-01T00:00:00.000Z").getMillis
    )
  }

  it should "Generate weekly buckets Sun-Tue" in {
    val interval = new Interval(DateTime.parse("2020-05-10T00:00:00.000Z"), DateTime.parse("2020-05-26T00:00:00.000Z"))
    val weeklyBuckets = Granularities.WEEK.getIterable(interval).asScala.map(b => b.getEndMillis)

    weeklyBuckets.toArray shouldEqual Array(
      DateTime.parse("2020-05-11T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-18T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-25T00:00:00.000Z").getMillis,
      DateTime.parse("2020-06-01T00:00:00.000Z").getMillis
    )
  }

  it should "Generate (daily buckets]" in {
    val interval = new Interval(DateTime.parse("2020-05-11T00:00:00.000Z"), DateTime.parse("2020-05-18T00:00:00.000Z"))
    val dailyBuckets = Granularities.DAY.getIterable(interval).asScala.map(b => b.getEndMillis)

    dailyBuckets.toArray shouldEqual Array(
      DateTime.parse("2020-05-12T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-13T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-14T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-15T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-16T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-17T00:00:00.000Z").getMillis,
      DateTime.parse("2020-05-18T00:00:00.000Z").getMillis
    )
  }
}