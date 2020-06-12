package net.ndolgov.druid.forecast.aggregation

import net.ndolgov.druid.forecast.aggregation.AggregateFunctions.IdentityElement
import net.ndolgov.druid.forecast.RowBatchIterator
import net.ndolgov.druid.forecast.regression.Forecaster
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema
import org.apache.druid.server.grpc.common.{RowBatch, RowBatchReaders}
import org.apache.druid.server.grpc.common.RowBatchReaders.RowBuffer
import org.joda.time.{DateTime, Interval}

import scala.annotation.tailrec

/**
  * A data processing operator that can be composed into a pipeline believed to be more efficient than the Volcano iterators one.
  * @tparam A the type of the unit of processing returned by an operator
  */
trait Op[A] {
  type OpConsumer = A => Unit

  /**
    * Apply this operator to input data and return the results iteratively by calling a given consumer
    * @param consumer the consumer of operator output
    */
  def exec(consumer: OpConsumer): Unit
}

/**
  * Strictly speaking aggregation-style logic is expensive enough to be implemented inside Druid. This is a stop-gap
  * approach which will pay the price of serialization and data transfer for the sake of simplicity. Once we
  * understand the functionality enough we'll seriously consider a "Druid contribution".
  *
  * The current implementation assumes that if there's a gap (i.e. historic data missing for a day) then it is present
  * in ALL measure time series. This allows to rely on a shared timestamp vector for all the measures.
  * In practice gaps are believed to be very unusual.
  *
  * "Today" is supposed to be the first day of a forecast. Elsewhere in the system the last available historic data day
  * is known by tenant. This information is not easily accessible to the report service right now.
  *
  * The general approach was inspired by "How to Architect a Query Compile, Revisited" by Tiark Rompf.
  * See https://www.cs.purdue.edu/homes/rompf/papers/tahboub-sigmod18.pdf for a whole lot of goodness.
  *
  * todo make sure Dims and Metrics are in canonical order
  */
object Ops {
  def source(schema: QuerySchema, batches: RowBatchIterator): Op[RowBuffer] =
    new FromRowBatchIteratorSourceOp(schema, batches)

  def sink(op: Op[RowBuffer]): Op[RowBuffer] = new SinkOp(op)

  def groupBy(op: Op[RowBuffer], nMeasures: Int, forecastBaseDays: Int = 90, forecastDays: Int = 365): Op[HashGroup] =
    new GroupByDimensionsOp(op, nMeasures, forecastBaseDays, forecastDays)

  def forecast(op: Op[HashGroup], forecaster: Forecaster, today: DateTime, daysToForecast: Int): Op[RowBuffer] =
    new ForecastOp(op, forecaster, today, daysToForecast)

  def aggregate(op: Op[RowBuffer], aggFuns: Seq[AggregateFunction], bucketsFactory: Iterable[Interval]): Op[RowBuffer] =
    new AggregateMeasuresOp(op, aggFuns, bucketsFactory)
}

/** Consume rows and print them out */
private final class SinkOp(op: Op[RowBuffer]) extends Op[RowBuffer] {
  override def exec(consumer: RowBuffer => Unit): Unit = {
    op.exec { row =>
      println(toString(row))
    }
  }

  private def toString(row: RowBuffer): String =
    s"(${new DateTime(row.timestamp)}|${row.dimensions.mkString("|")} -> ${row.longMetrics.mkString("|")} | ${row.doubleMetrics.mkString("|")})"
}

/**
  * Assumption: all metrics are aligned on the same timestamps and so have gaps at the same time
  */
private final class FromRowBatchIteratorSourceOp(schema: QuerySchema, batches: RowBatchIterator) extends Op[RowBuffer] {
  private val batchReader = RowBatchReaders.reader(schema)

  override def exec(consumer: OpConsumer): Unit = {
    while (batches.hasNext) {
      exec(consumer, batches.next())
    }
  }

  private def exec(consumer: OpConsumer, batch: RowBatch) : Unit = {
    val reader = batchReader.reset(batch)

    while (reader.hasNext) {
      consumer(reader.next())
    }
  }
}

/**
  * Consume rows and produce partitions found by grouping by all dimensions
  * @param op historic data row source
  * @param nMeasures the number of measures
  * @param forecastBaseDays the maximum number of days in historic data
  * @param forecastDays the number of days to forecast for
  */
private final class GroupByDimensionsOp(op: Op[RowBuffer], nMeasures: Int, forecastBaseDays: Int, forecastDays: Int) extends Op[HashGroup] {
  override def exec(consumer: HashGroup => Unit): Unit = {
    val groups = HashGroups(nMeasures, forecastBaseDays, forecastDays)

    op.exec(row => groups.get(row.dimensions).append(row))

    groups.iterator().foreach(group => consumer(group))
  }
}

/**
  * Consume historic data and produce a forecast for a given number of days
  * TODO consider parallel processing
  * @param op historic data row group source
  * @param forecaster time series forecasting logic to use
  * @param today the date that represents today
  * @param daysToForecast the number of days to forecast (including today)
  */
private final class ForecastOp(op: Op[HashGroup], forecaster: Forecaster, today: DateTime, daysToForecast: Int) extends Op[RowBuffer] {
  override def exec(consumer: OpConsumer): Unit = {
    op.exec { group =>
      val row = new RowBuffer(group.dimensions, Array.ofDim(group.measures.length), Array.empty) // a new one for each group to signal the consumer

      val endOfHistory = group.size
      val oracles = group.measures.map(measure => forecaster.forecast(group.timestamps, 0, endOfHistory, measure, 0, endOfHistory))

      for (dayMillis <- ForecastPeriods.forecastDayTimestamps(today, daysToForecast)) {
        row.timestamp = dayMillis

        for (measureIdx <- group.measures.indices) {
          row.doubleMetrics(measureIdx) = oracles(measureIdx)(dayMillis)
        }

        consumer(row)
      }
    }
  }
}

/** Consume forecast rows and produce a cumulative row for each aggregated partition. The forecast rows are assumed
  * to be sorted on all dimensions and use a different buffer instance for every unique dimension combination. So
  * it assumes its input was produced by iterating rows of hash groups one group at a time. */
private final class AggregateMeasuresOp(op: Op[RowBuffer], aggFuns: Seq[AggregateFunction], bucketsFactory: Iterable[Interval]) extends Op[RowBuffer] {
  override def exec(consumer: OpConsumer): Unit = {
    var previousRow: RowBuffer = null

    var groupAggregator: GroupAggregator = null

    val identityElements: Array[IdentityElement] = aggFuns.map(f => f.identity).toArray

    op.exec(row => {
      if (groupAggregator == null) {
        val aggregatedBucket = new RowBuffer(Array.ofDim(row.dimensions.length), identityElements.clone(), Array.empty)
        groupAggregator = new GroupAggregator(row.dimensions, bucketsFactory.iterator, aggregatedBucket)
      }

      groupAggregator = groupAggregator.consume(row)
    })
    if (groupAggregator != null) {
      groupAggregator.flushLastAggregatedBucket()
    }

    /**
      *
      * @param dimensions the dimension values of the hash group being aggregated.
      * @param buckets the buckets to aggregate into
      * @param aggregatedBucket the buffer to collect aggregated values in
      */
    final class GroupAggregator(dimensions: Array[Int], buckets: Iterator[Interval], aggregatedBucket: RowBuffer) {
      if (!buckets.hasNext) {
        throw new IllegalArgumentException("No buckets")
      }

      private var bucket = buckets.next() // the current bucket - collect measures up to its end, emit with its start timestamp
      private var bucketEnd = bucket.getEndMillis
      private var rowCount = 0 // the number of rows collected by the current aggregator

      aggregatedBucket.timestamp = bucket.getStartMillis
      Array.copy(dimensions, 0, aggregatedBucket.dimensions, 0, dimensions.length) // reset for the new group

      @tailrec
      def consume(row: RowBuffer): GroupAggregator = {
        if (row == previousRow) { // still the same group
          if (row.timestamp < bucketEnd) { // still the same bucket
            for (measureIdx <- aggregatedBucket.doubleMetrics.indices) {
              val prev = aggregatedBucket.doubleMetrics(measureIdx)
              aggregatedBucket.doubleMetrics(measureIdx) = aggFuns(measureIdx).aggFun(prev, row.doubleMetrics(measureIdx))
            }

            rowCount += 1
            this
          } else { // perform bucket housekeeping and then trigger row processing
            emitAggregatedBucket(bucket.getStartMillis)

            if (buckets.hasNext) {
              bucket = buckets.next()
            } else {
              //logger.warn(s"Exhausted buckets with $bucket at timestamp ${row.timestamp}") // TODO or simply skip the rest?
              bucket = new Interval(bucket.getEnd, DateTime.now().plusYears(10)) // "infinity" to put all remaining rows into a single bucket
            }

            bucketEnd = bucket.getEndMillis
            Array.copy(identityElements, 0, aggregatedBucket.doubleMetrics, 0, identityElements.length)

            consume(row) // actually process the row once the housekeeping is done
          }
        } else { // perform group housekeeping and then trigger row processing
          if (previousRow == null) { // the first row ever
            previousRow = row
            consume(row) // process as the first row in the next bucket
          } else { // the first row of a different hash group, except for the very first group
            emitAggregatedBucket(bucket.getStartMillis)

            val aggregator = new GroupAggregator(row.dimensions, bucketsFactory.iterator, aggregatedBucket)
            previousRow = row
            aggregator.consume(row) // process as the first row in the new group
          }
        }
      }

      private def emitAggregatedBucket(bucketTimestamp: Long): Unit = {
        if (rowCount > 0) {
          aggregatedBucket.timestamp = bucketTimestamp
          consumer(aggregatedBucket)

          rowCount = 0
        }
      }

      // make possible to emit the very last aggregated chunk when there are no more input rows to trigger it
      def flushLastAggregatedBucket(): Unit = {
        emitAggregatedBucket(bucket.getStartMillis)
      }
    }
  }
}
