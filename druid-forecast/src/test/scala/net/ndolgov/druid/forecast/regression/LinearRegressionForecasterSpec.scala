package net.ndolgov.druid.forecast.regression

import org.joda.time.DateTime
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class LinearRegressionForecasterSpec extends FlatSpec with GivenWhenThen with Matchers {
  "forecast" should "append predictions to historic data" in {
    val forecast = new LinearRegressionForecaster().forecast(
      Array(
        DateTime.parse("2018-10-29T00:00:00.000Z").getMillis,
        DateTime.parse("2018-10-30T00:00:00.000Z").getMillis,
        DateTime.parse("2018-10-31T00:00:00.000Z").getMillis,
        DateTime.parse("2018-11-01T00:00:00.000Z").getMillis),
      Array(120.0, 122.0, 124.0, 126.0))

    //val f = forecast()

    Array(
      DateTime.parse("2018-11-02T00:00:00.000Z").getMillis,
      DateTime.parse("2018-11-03T00:00:00.000Z").getMillis,
      DateTime.parse("2018-11-04T00:00:00.000Z").getMillis).
    map(t => forecast(t)) shouldEqual Seq(128.0, 130.0, 132.0)

    Array(
      DateTime.parse("2018-11-03T00:00:00.000Z").getMillis,
      DateTime.parse("2018-11-04T00:00:00.000Z").getMillis).
    map(t => forecast(t)) shouldEqual Seq(130.0, 132.0)

    forecast(DateTime.parse("2018-11-04T00:00:00.000Z").getMillis) shouldEqual 132.0
  }
}
