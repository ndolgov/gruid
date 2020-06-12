package net.ndolgov.druid.forecast.regression

/** Forecast with "y = beta * x + alpha" */
final class LinearRegressionForecaster extends Forecaster {
  private def linearRegression(yList: Seq[Double]) = {
    val xList = yList.indices.toList.map(_.toDouble)
    val size = yList.size
    val x = xList.last / 2
    val y = yList.sum / size
    val xy = yList.zip(xList).map(pair => pair._1 * pair._2).sum / size
    val x2 = xList.map(num => num * num).sum / size
    val alpha = (xy - x * y) / (x2 - x * x)
    val beta = y - alpha * x
    (alpha, beta)
  }

  override def forecast(x: Array[Timestamp], fromX: Int, toX: Int, y: Array[Value], fromY: Int, toY: Int): Oracle = {
    if (x.length == 0) {
      throw new IllegalArgumentException("No historic data found")
    }
    if (x.length == 1) {
      return _ => y(0)
    }
    if ((x.length != y.length) || ((toY - fromY) != (toX - fromX))) {
      throw new IllegalArgumentException(s"Different numbers of timestamps ${x.length} and values ${y.length}")
    }

    // first pass
    var sumx = 0.0
    //var sumx2 = 0.0
    for (i <- fromX until toX) {
      //sumx2 += x(i)*x(i)
      sumx += x(i)
    }

    var sumy = 0.0
    for (i <- fromY until toY) {
      sumy += y(i)
    }

    val nDatapoints = toY - fromY
    val xbar = sumx / nDatapoints
    val ybar = sumy / nDatapoints

    // second pass: compute summary statistics
    var xxbar = 0.0
    //var yybar = 0.0
    for (i <- fromX until toX) {
      xxbar += (x(i) - xbar) * (x(i) - xbar)
    }
    var xybar = 0.0
    for (i <- fromY until toY) {
      //yybar += (y(i) - ybar) * (y(i) - ybar)
      xybar += (x(i) - xbar) * (y(i) - ybar)
    }
    val slope = xybar / xxbar
    val intercept = ybar - slope * xbar

    //TODO: remove this urgent fix (kludge)
    if (slope.isNaN || intercept.isNaN) {
      throw new IllegalStateException("NaN encountered")
    }

    time: Timestamp => slope * time + intercept
  }

  /** Translated from https://algs4.cs.princeton.edu/14analysis/LinearRegression.java */
  override def forecast(x: Array[Timestamp], y: Array[Value]): Oracle = {
    forecast(x, 0, x.length, y, 0, y.length)

  }
}
