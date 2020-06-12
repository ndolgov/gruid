package net.ndolgov.druid

import org.apache.druid.java.util.common.granularity.Granularity
import org.joda.time.Interval

package object forecast {
  case class ForecastMetric(name: String, aggFunction: String)

  case class ForecastDimension(name: String)

  //    historic data   forecast
  // |----------------|-----------|
  //
  // ^-forecast base  ^today     ^+daysToForecast
  //
  case class ForecastPeriod(today: Interval, forecastBase: Int, daysToForecast: Int)

  /**
   * @param dimensions dimensions to group by for aggregation
   * @param metrics metrics to forecast
   * @param period forecast period
   * @param granularity the granularity of the forecast to produce
   */
  case class ForecastRequest(
    druidQuery: String,
    dimensions: Seq[ForecastDimension],
    metrics: Seq[ForecastMetric],
    period: ForecastPeriod,
    granularity: Granularity
  )
}
