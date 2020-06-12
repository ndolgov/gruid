package net.ndolgov.druid.forecast

import java.util.concurrent.TimeUnit

import net.ndolgov.druid.forecast.regression.LinearRegressionForecaster
import org.apache.druid.java.util.common.granularity.Granularities
import org.apache.druid.server.grpc.client.GrpcClient
import org.apache.druid.server.grpc.common.TestRowBatchQueryServices
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class DruidForecastServiceSpec extends FlatSpec with GivenWhenThen with Matchers {
  private val logger = LoggerFactory.getLogger(classOf[DruidForecastServiceSpec])

  private val Port = 20002
  private val Hostname = "localhost"

  private val groupByDimQuery: String = "TODO"

  "forecast service" should "wire up and execute op chain" in {
    val executor = TestRowBatchQueryServices.executor("rpc-client-%d")

    val server = TestDruidServer.server(Hostname, Port)
    server.start()

    val May29 = 1590710400000L
    val timeUtils = TimeUtils(() => May29)

    val client = new GrpcClient(Hostname, Port, executor)

    val service = new DruidForecastService(
      ForecastService(new LinearRegressionForecaster(), timeUtils),
      client,
      ExecutionContext.fromExecutor(executor),
      5000
    )

    val request = ForecastRequest(
      groupByDimQuery,
      Seq(ForecastDimension("Country"), ForecastDimension("Product")),
      Seq(ForecastMetric("Count", "min"), ForecastMetric("Weight", "max")),
      ForecastPeriod(timeUtils.todayInterval(), 5, 3),
      Granularities.DAY
    )

    try {
        val future = service.execute(request)
        Await.result(future, Duration.create(100, TimeUnit.SECONDS))
      } finally {
      client.close()
      server.stop()
    }
  }
}
