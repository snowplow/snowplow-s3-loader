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

import scala.concurrent.duration.FiniteDuration

import cats.implicits._
import cats.effect.kernel.{Async, Resource, Sync}

import fs2.Stream

import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}

import com.snowplowanalytics.snowplow.streams.SourceAndAck

trait Metrics[F[_]] {
  def addCount(count: Long): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setE2ELatency(latency: FiniteDuration): F[Unit]

  def scrape: F[String]
  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics, sourceAndAck: SourceAndAck[F]): Resource[F, Metrics[F]] =
    CommonMetrics.build[F](config.statsd, config.prometheus).evalMap { entries =>
      for {
        count <- entries.counter("count")
        latency <- entries.timer("latency_millis", sourceAndAck.currentStreamLatency)
        e2eLatency <- entries.timer("e2e_latency_millis", Sync[F].pure(None))
        latencyCollectorToLoad <- entries.gauge("latency_collector_to_load") // Legacy metric
      } yield new Metrics[F] {
        def addCount(n: Long): F[Unit]                = count.add(n)
        def setLatency(l: FiniteDuration): F[Unit]    = latency.record(l)
        def setE2ELatency(l: FiniteDuration): F[Unit] = e2eLatency.record(l) *> latencyCollectorToLoad.set(l.toSeconds)
        def scrape: F[String]                         = entries.scrape
        def report: Stream[F, Nothing]                = entries.report
      }
    }
}
