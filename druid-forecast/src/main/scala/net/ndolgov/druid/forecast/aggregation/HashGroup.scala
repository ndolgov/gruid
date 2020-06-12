package net.ndolgov.druid.forecast.aggregation

import java.util
import java.util.ConcurrentModificationException

import gnu.trove.map.TIntObjectMap
import gnu.trove.map.hash.TIntObjectHashMap
import org.apache.druid.java.util.common.granularity.Granularities
import org.apache.druid.server.grpc.common.RowBatchReaders.RowBuffer
import org.joda.time.{DateTime, Interval}

import scala.collection.JavaConverters._

/** Measure time series collected from the rows with identical dimension values */
case class HashGroup(hash: Int, dimensions: Array[Int], timestamps: Array[Long], measures: Array[Array[Double]]/*, todayIdx: Int*/) {
  private var offset = 0

  // buffer.dimensions are ignored
  def append(buffer: RowBuffer): Unit = {
    timestamps(offset) = buffer.timestamp

    for (i <- measures.indices)
      measures(i)(offset) = buffer.doubleMetrics(i)

    offset += 1
  }

  def size: Int = offset

  override def hashCode(): Int = hash

  override def equals(obj: Any): Boolean = {
    val that = obj.asInstanceOf[HashGroup]
    this.dimensions.sameElements(that.dimensions)
  }

  override def toString: String = s"{HashGroup:Ds=${dimensions.mkString("|")}, size=$size, hash=$hash}"
}

object HashGroup {
  /**
    * @param nMeasures expected number of measures to collect
    * @param forecastBaseDays the maximum number of days in historic data
    * @param forecastDays the maximum number of days in forecast
    * @return an empty group for a given dimension combination. Size up measure buffers for the worst case based on the
    *         number of days in history and forecast.
    */
  def apply(hash: Int, dimensions: Array[Int], nMeasures: Int, forecastBaseDays: Int, forecastDays: Int): HashGroup = {
    val length = forecastBaseDays + forecastDays

    val measures = Array.ofDim[Array[Double]](nMeasures)
    for (i <- 0 until nMeasures) {
      measures(i) = Array.ofDim[Double](length)
    }

    HashGroup(hash, dimensions, Array.ofDim[Long](length), measures)
  }

  def hash(dimensions: Array[Int]): Int = util.Arrays.hashCode(dimensions)
}

/** A traversable collection of HashGroups keyed by dimensions */
trait HashGroups {
  /**
    * @param dimensions dictionary-encoded dimension values in the canonical order
    * @return an existing group if found, a newly create one otherwise
    */
  def get(dimensions: Array[Int]): HashGroup

  def iterator(): Iterator[HashGroup]

  def size(): Int
}

private final class TroveHashGroups(initialCapacity: Int, nMeasures: Int, forecastBaseDays: Int, forecastDays: Int) extends HashGroups {
  private val groups: TIntObjectMap[HashGroup] = new TIntObjectHashMap[HashGroup](initialCapacity)

  override def get(dimensions: Array[Int]): HashGroup = {
    val key = HashGroup.hash(dimensions)
    val existing = groups.get(key)

    if (existing == null) {
      val group = HashGroup(key, dimensions.clone(), nMeasures, forecastBaseDays, forecastDays)
      if (groups.put(key, group) != null) {
        throw new ConcurrentModificationException(s"Group $key already exists")
      }
      group
    } else {
      existing
    }
  }

  override def iterator(): Iterator[HashGroup] = groups.valueCollection().iterator().asScala

  override def size(): Int = groups.size()
}

object HashGroups {
  def apply(nMeasures: Int, forecastBaseDays: Int = 90, forecastDays: Int = 365, initialCapacity:Int = 1024): HashGroups =
    new TroveHashGroups(initialCapacity, nMeasures, forecastBaseDays, forecastDays)
}

object ForecastPeriods {
  /**
    * @param today the date that represents "today"
    * @param daysToForecast how many day timestamps to generate (including today)
    * @return timestamps representing the days of a forecast starting from today
    */
  def forecastDayTimestamps(today: DateTime, daysToForecast: Int): Iterator[Long] = {
    val firstDayAfterForecast = today.plusDays(daysToForecast)
    val period = new Interval(today, firstDayAfterForecast)
    Granularities.DAY.getIterable(period).asScala.iterator.map(interval => interval.getStartMillis)
  }
}