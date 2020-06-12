package net.ndolgov.druid.forecast.aggregation

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class AggregateFunctionsSpec extends FlatSpec with GivenWhenThen with Matchers {
  it should "parse from string" in {
    AggregateFunctions("sum") shouldEqual Sum
    AggregateFunctions("min") shouldEqual Min
    AggregateFunctions("max") shouldEqual Max
    AggregateFunctions("avg") shouldEqual Avg
    AggregateFunctions("distinct") shouldEqual Distinct
  }

  it should "apply binary op" in {
    Sum.aggFun(10, Sum.identity) shouldEqual 10
    Sum.aggFun(10, 2) shouldEqual 12

    Min.aggFun(10, Min.identity) shouldEqual 10
    Min.aggFun(10, 2) shouldEqual 2

    Max.aggFun(10, Max.identity) shouldEqual 10
    Max.aggFun(10, 2) shouldEqual 10
  }
}
