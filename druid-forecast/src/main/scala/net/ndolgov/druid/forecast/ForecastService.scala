package net.ndolgov.druid.forecast

import net.ndolgov.druid.forecast.aggregation.{AggregateFunctions, Op, Ops}
import net.ndolgov.druid.forecast.regression.Forecaster
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema
import org.apache.druid.server.grpc.common.RowBatchReaders.RowBuffer

import scala.collection.JavaConverters._

/** The Facade of forecast report functionality */
trait ForecastService {
  /**
    * @param schema input/historic data schema
    * @param input input data chunk iterator
    * @param request forecasting parameters
    */
  def forecast(schema: QuerySchema, input: RowBatchIterator, request: ForecastRequest): Op[RowBuffer]
}

object ForecastService {
  def apply(forecaster: Forecaster, timeUtils: TimeUtils): ForecastService =
    new StreamingForecastService(forecaster, timeUtils)
}

private[forecast] class StreamingForecastService(forecaster: Forecaster, timeUtils: TimeUtils) extends ForecastService {
  override def forecast(schema: QuerySchema, input: RowBatchIterator, request: ForecastRequest) : Op[RowBuffer] = {
    Ops.sink(
      Ops.aggregate(
        Ops.forecast(
          Ops.groupBy(
            Ops.source(schema, input),
            request.metrics.size,
            request.period.forecastBase,
            request.period.daysToForecast),
          forecaster,
          request.period.today.getStart,
          request.period.daysToForecast),
        request.metrics.map(metric => AggregateFunctions(metric.aggFunction)),
        request.granularity.getIterable(timeUtils.nextNdaysInterval(request.period.daysToForecast)).asScala)
      )
  }
}