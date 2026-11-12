/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.blob.core

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect

import fs2.Stream

import org.specs2.Specification

import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, SourceAndAck}

// common-streams validates metric names when they are registered, at runtime, so a bad name
// compiles fine and only fails when the loader starts. Building the real `Metrics` here catches it.
class MetricsSpec extends Specification with CatsEffect {

  def is = s2"""
  The scrape endpoint should:
    expose the latency metrics with the unit suffix prometheus gives them $e1
  """

  // common-streams appends the unit suffix itself, so the loader registers bare names and the
  // series that reaches prometheus is not the string in `Metrics.build`.
  def e1 =
    Metrics
      .build[IO](Config.Metrics(statsd = None, prometheus = CommonMetrics.PrometheusConfig(tags = Map.empty)), 2.minutes, dummySource)
      .use { metrics =>
        for {
          _ <- metrics.setLatency(7.seconds)
          _ <- metrics.setE2ELatency(3.minutes)
          scraped <- metrics.scrape
        } yield {
          val names = scraped.linesIterator.filterNot(_.startsWith("#")).map(_.trim.takeWhile(c => c != ' ' && c != '{')).toSet
          names must containAllOf(
            List(
              "latency_seconds",
              "latency_observations_total",
              "e2e_latency_seconds_count"
            )
          )
        }
      }

  private def dummySource: SourceAndAck[IO] = new SourceAndAck[IO] {
    def stream(config: EventProcessingConfig[IO], processor: EventProcessor[IO]): Stream[IO, Nothing] = Stream.never[IO]
    def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus]         = IO.pure(SourceAndAck.Healthy)
    def currentStreamLatency: IO[Option[FiniteDuration]]                                              = IO.pure(None)
  }
}
