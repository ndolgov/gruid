package net.ndolgov.druid.forecast.regression

/** A value returned by forecasting logic (with the corresponding confidence interval) */
case class ForecastPoint(sup: Double, mean: Double, inf: Double)

/**
  * Given historic data for a time series forecast its continuation for a given number of time units. The data can have
  * arbitrary gaps but must have at least one value.
  *
  * TODO consider using int "offset in days" instead of milliseconds
  * TODO consider https://www.analyticsvidhya.com/blog/2018/02/time-series-forecasting-methods/
  */
trait Forecaster {
  type Timestamp = Long
  type Value = Double
  type Oracle = Timestamp => Value

  /**
    * @param x timestamps (in the same unit as the expected output)
    * @param y historic values corresponding to the timestamps
    * @return a function that can forecast values given some future timestamps
    */
  def forecast(x: Array[Timestamp], y: Array[Value]): Oracle

  /**
    *
    * @param x
    * @param fromX, inclusive
    * @param toX, exclusive
    * @param y
    * @param fromY, inclusive
    * @param toY, exclusive
    * @return
    */
  def forecast(x: Array[Timestamp], fromX: Int, toX: Int, y: Array[Value], fromY: Int, toY: Int): Oracle
}
