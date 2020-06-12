package net.ndolgov.druid.forecast

import java.time.Duration

import org.joda.time.{DateTimeZone, Interval}
import org.joda.time.format.DateTimeFormat //DateTimeFormatter

object TimeUtil {
  def toLong(timeline: String): Long = Duration.parse(timeline).toMillis

  def truncHours(timestamp: Long): Long = {
    timestamp - timestamp % (1000 * 60 * 60 * 24)
  }

  def dateToLong(date: String, mask: String = "yyyy-MM-dd"): Long = {
    DateTimeZone.setDefault(DateTimeZone.UTC)
    val dtf = DateTimeFormat.forPattern(mask)
    dtf.parseDateTime(date).getMillis
  }
}

/** Time utilities that rely on a time provider for deterministic tests */
final class TimeUtils(clock: () => Long) {
  /** @return a period of N days starting today */
  def nextNdaysInterval(days: Int): Interval = {
    DateTimeZone.setDefault(DateTimeZone.UTC)
    val currentTime = clock()
    val currentTimeWithoutHours = TimeUtil.truncHours(currentTime)
    new Interval(
      currentTimeWithoutHours,
      currentTimeWithoutHours + TimeUtil.toLong(s"P${days}D"))
  }

  def todayInterval(): Interval = {
    DateTimeZone.setDefault(DateTimeZone.UTC)
    val currentTime = clock()
    val currentTimeWithoutHours = TimeUtil.truncHours(currentTime)
    new Interval(currentTimeWithoutHours, currentTimeWithoutHours + TimeUtil.toLong("P1D"))
  }
}

object TimeUtils {
  def apply(): TimeUtils = new TimeUtils(() => System.currentTimeMillis())

  def apply(clock: () => Long): TimeUtils = new TimeUtils(clock)
}