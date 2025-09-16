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

import scala.concurrent.duration.{Duration, FiniteDuration}

import cats.implicits._

import cats.effect.kernel.{Async, Ref, Sync}

import fs2.Stream

import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}

trait Metrics[F[_]] {
  def addCount(count: Int): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setE2ELatency(latency: FiniteDuration): F[Unit]

  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics): F[Metrics[F]] =
    Ref[F].of(State.empty).map(impl(config, _))

  private case class State(
    count: Int,
    latency: FiniteDuration,
    e2eLatency: Option[FiniteDuration]
  ) extends CommonMetrics.State {
    def toKVMetrics: List[CommonMetrics.KVMetric] =
      List(
        KVMetric.Count(count),
        KVMetric.Latency(latency)
      ) ++
        e2eLatency.map(KVMetric.E2ELatency(_)) ++
        e2eLatency.map(KVMetric.LatencyCollectorToLoad(_))
  }

  private object State {
    def empty: State = State(0, Duration.Zero, None)
  }

  private def impl[F[_]: Async](config: Config.Metrics, ref: Ref[F, State]): Metrics[F] =
    new CommonMetrics[F, State](ref, Sync[F].pure(State.empty), config.statsd) with Metrics[F] {
      def addCount(count: Int): F[Unit] =
        ref.update(s => s.copy(count = s.count + count))
      def setLatency(latency: FiniteDuration): F[Unit] =
        ref.update(s => s.copy(latency = s.latency.max(latency)))
      def setE2ELatency(e2eLatency: FiniteDuration): F[Unit] =
        ref.update { s =>
          val newLatency = s.e2eLatency.fold(e2eLatency)(_.max(e2eLatency))
          s.copy(e2eLatency = Some(newLatency))
        }
    }

  private object KVMetric {

    final case class Count(v: Int) extends CommonMetrics.KVMetric {
      val key        = "count"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class Latency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key        = "latency_millis"
      val value      = d.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    final case class E2ELatency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key        = "e2e_latency_millis"
      val value      = d.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    // Legacy metric

    final case class LatencyCollectorToLoad(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key        = "latency_collector_to_load"
      val value      = d.toSeconds.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }
  }
}
