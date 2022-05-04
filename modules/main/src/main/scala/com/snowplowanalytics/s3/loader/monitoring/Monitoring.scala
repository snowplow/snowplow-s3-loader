/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
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
