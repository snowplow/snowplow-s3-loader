package com.snowplowanalytics.s3.loader.monitoring

import scala.util.{ Success, Failure }
import scala.concurrent.ExecutionContext

import cats.Id

import com.snowplowanalytics.snowplow.scalatracker.Tracker

import com.snowplowanalytics.s3.loader.Config
import com.snowplowanalytics.s3.loader.processing.Batch.Meta

class Monitoring(snowplow: Option[Tracker[Id]], statsD: Option[Config.StatsD]) {

  private implicit val EC: ExecutionContext =
    scala.concurrent.ExecutionContext.global

  def isSnowplowEnabled: Boolean =
    snowplow.isDefined

  def isStatsDEnabled: Boolean =
    statsD.isDefined

  def viaSnowplow(track: Tracker[Id] => Unit): Unit =
    snowplow.foreach(track)

  def report(meta: Meta): Unit =
    statsD.foreach { config =>
      StatsD.report(config)(meta).onComplete {
        case Success(_) => ()
        case Failure(error) => System.err.println(error)
      }
    }

  /**
   * Send a startup event and attach a shutdown hook
   * No-op is Snowplow is not configured
   */
  def initTracking(): Unit =
    snowplow.foreach { tracker =>
      SnowplowTracking.initializeSnowplowTracking(tracker)
    }
}

object Monitoring {
  def build(config: Option[Config.Monitoring]): Monitoring =
    config match {
      case Some(Config.Monitoring(snowplow, metrics)) =>
        val tracker = snowplow.map { snowplowConfig =>
          SnowplowTracking.initializeTracker(snowplowConfig)
        }
        new Monitoring(tracker, metrics.flatMap(_.statsd))
      case None => new Monitoring(None, None)
    }
}
