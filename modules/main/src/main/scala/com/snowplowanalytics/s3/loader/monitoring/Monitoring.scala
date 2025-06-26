/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.s3.loader.monitoring

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext

import org.slf4j.LoggerFactory

import cats.Id

import io.sentry.{Sentry, SentryClient}

import com.snowplowanalytics.snowplow.scalatracker.Tracker

import com.snowplowanalytics.s3.loader.Config
import com.snowplowanalytics.s3.loader.processing.Batch.Meta

class Monitoring(snowplow: Option[Tracker[Id]], statsD: Option[Config.StatsD], sentry: Option[SentryClient]) {

  private implicit val EC: ExecutionContext =
    scala.concurrent.ExecutionContext.global

  private val logger = LoggerFactory.getLogger(getClass)

  def isSnowplowEnabled: Boolean =
    snowplow.isDefined

  def isStatsDEnabled: Boolean =
    statsD.isDefined

  def viaSnowplow(track: Tracker[Id] => Unit): Unit =
    snowplow.foreach(track)

  def report(meta: Meta): Unit =
    statsD.foreach { config =>
      StatsD.report(config)(meta).onComplete {
        case Success(_)     => logger.debug(s"Metrics with ${meta.count} records have been successfully reported")
        case Failure(error) => logger.error(s"Could not send metrics with ${meta.count} records", error)
      }
    }

  /**
   * Send a startup event and attach a shutdown hook
   * No-op if Snowplow is not configured
   */
  def initTracking(): Unit =
    snowplow.foreach { tracker =>
      SnowplowTracking.initializeSnowplowTracking(tracker)
    }

  def captureError(error: Throwable): Unit =
    sentry.foreach { client =>
      client.sendException(error)
    }
}

object Monitoring {
  def build(config: Option[Config.Monitoring]): Monitoring =
    config match {
      case Some(Config.Monitoring(snowplow, sentry, metrics)) =>
        val tracker = snowplow.map { snowplowConfig =>
          SnowplowTracking.initializeTracker(snowplowConfig)
        }
        val sentryClient = sentry.map { sentryConfig =>
          Sentry.init(sentryConfig.dsn.toString)
        }
        new Monitoring(tracker, metrics.flatMap(_.statsd), sentryClient)
      case None => new Monitoring(None, None, None)
    }
}
