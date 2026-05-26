package phd.adaptivecontrol.util

import java.time.{Instant, ZoneId, ZonedDateTime}


/**
  * TimeUtils
  *
  * Utility functions for:
  *   - event-time handling
  *   - timestamp conversion
  *   - debugging temporal behavior
  *   - adaptive window/watermark logic support
  */
object TimeUtils {

  // ============================================================
  // UTC Zone
  // ============================================================
  private val UTC: ZoneId =
    ZoneId.of("UTC")

  // ============================================================
  // Epoch millis -> Instant
  // ============================================================
  def toInstant(timestamp: Long): Instant = {

    Instant.ofEpochMilli(timestamp)
  }

  // ============================================================
  // Epoch millis -> Human readable time
  // ============================================================
  def toUTCString(timestamp: Long): String = {

    ZonedDateTime
      .ofInstant(toInstant(timestamp), UTC)
      .toString
  }

  // ============================================================
  // Time difference (ms)
  // ============================================================
  def diffMs(t1: Long, t2: Long): Long = {

    math.abs(t1 - t2)
  }

  // ============================================================
  // Check lateness relative to watermark
  // ============================================================
  def isLateEvent(eventTime: Long, watermark: Long): Boolean = {

    eventTime < watermark
  }

  // ============================================================
  // Convert seconds to milliseconds
  // ============================================================
  def secondsToMillis(seconds: Long): Long = {

    seconds * 1000L
  }

  // ============================================================
  // Convert minutes to milliseconds
  // ============================================================
  def minutesToMillis(minutes: Long): Long = {

    minutes * 60L * 1000L
  }
}
