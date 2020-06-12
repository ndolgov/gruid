package net.ndolgov.druid.forecast.aggregation

import net.ndolgov.druid.forecast.aggregation.AggregateFunctions.{AVG, AggFun, DISTINCT, IdentityElement, MAX, MIN, SUM}

// todo move to a higher level
object AggregateFunctions {
  type AggFun = (Double, Double) => Double
  type IdentityElement = Double

  val SUM: AggFun = (a, b) => a + b
  val MIN: AggFun = (a, b) => Math.min(a, b)
  val MAX: AggFun = (a, b) => Math.max(a, b)
  val AVG: AggFun = (_, _) => throw new UnsupportedOperationException("AVG aggregate function")
  val DISTINCT: AggFun = (_, _) => throw new UnsupportedOperationException("Distinct aggregate function")

  def apply(strAggFun: String): AggregateFunction = strAggFun match {
    case Min.name => Min

    case Max.name => Max

    case Sum.name => Sum

    case Avg.name => Avg

    case Distinct.name => Distinct

    case _ => throw new IllegalArgumentException(s"Unsupported aggregate function $strAggFun")
  }

  /**
    * todo use DruidAggregationType
    * @param af supported aggregate function
    * @return the Druid aggregation type as a string that can be used in Druid JSON requests for Double measures
    */
  def druidDoubleAggregator(af: AggregateFunction): String = af match {
    case Min => "doubleMin"

    case Max => "doubleMax"

    case Sum => "doubleSum"

    case _ => throw new IllegalArgumentException(s"No built-in Druid aggregator for $af")
  }

  /**
    * @param af supported aggregate function
    * @return the Druid aggregation type as a string that can be used in Druid JSON requests for Long measures
    */
  def druidLongAggregator(af: AggregateFunction): String = af match {
    case Min => "longMin"

    case Max => "longMax"

    case Sum => "longSum"

    case _ => throw new IllegalArgumentException(s"No built-in Druid aggregator for $af")
  }
}

sealed trait AggregateFunction { 
  def name: String
  def aggFun: AggFun
  def identity: IdentityElement}
case object Min extends AggregateFunction { val name = "min"; val aggFun = MIN; val identity = Double.MaxValue;}
case object Max extends AggregateFunction { val name = "max"; val aggFun = MAX; val identity = Double.MinValue; }
case object Sum extends AggregateFunction { val name = "sum"; val aggFun = SUM; val identity = 0.0; }
case object Avg extends AggregateFunction { val name = "avg"; val aggFun = AVG; val identity = Double.NaN; }
case object Distinct extends AggregateFunction { val name = "distinct"; val aggFun = DISTINCT; val identity = Double.NaN; }
