/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.s3.loader.monitoring

import java.time.{Duration, Instant}
import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import org.slf4j.LoggerFactory

import com.snowplowanalytics.s3.loader.Config
import com.snowplowanalytics.s3.loader.processing.Batch.Meta

object StatsD {

  val CollectorLatencyName = "latency_collector_to_load"
  val CountName = "count"
  val CollectorTstampIdx = 3

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val EC: ExecutionContext =
    scala.concurrent.ExecutionContext.global

  sealed trait KVMetric {
    def key: String
    def value: String
    def `type`: String
  }
  object KVMetric {
    final case class Gauge(key: String, value: String) extends KVMetric {
      def `type`: String = "g"
    }
    final case class Count(key: String, count: Int) extends KVMetric {
      def value: String = count.toString
      def `type`: String = "c"
    }
  }

  /**
   * Build a list of StatsD metrics from batch metadata
   * and send them to a configured statsd server
   * Metadata contains static information about the written
   * batch and `report` evaluates metrics on-fly, i.e.
   * calculates time delta between timestamps in meta and now
   *
   * @param config configured statsd
   * @param meta metadata to build metrics from
   */
  def report(config: Config.StatsD)(meta: Meta): Future[Unit] = {
    logger.debug(s"Sending $meta metrics")

    val result = Future.traverse(fromMeta(meta)) { metric =>
      val strMetric = statsDFormat(config, metric).getBytes(UTF_8)

      Future(new DatagramSocket).flatMap { socket =>
        Future {
          val ip = InetAddress.getByName(config.hostname)
          socket.send(
            new DatagramPacket(strMetric, strMetric.length, ip, config.port)
          )
        }.transform(s => Try(socket.close()).flatMap(_ => s))
      }
    }

    result.map(_ => ())
  }

  /** Build a list of metrics from metadata */
  def fromMeta(meta: Meta): List[KVMetric] =
    List(
      meta.earliestTstamp.map(getTstampMetrics),
      Some(KVMetric.Count(CountName, meta.count))
    ).flatten

  /** Build a metric with difference between a timestamp and current time */
  def getTstampMetrics(collectorTstamp: Instant): KVMetric = {
    val diff = Duration.between(collectorTstamp, Instant.now())
    KVMetric.Gauge(CollectorLatencyName, diff.toSeconds.toString)
  }

  private def statsDFormat(config: Config.StatsD, metric: KVMetric): String = {
    val tagStr = config.tags.map { case (k, v) => s"$k:$v" }.mkString(",")
    val prefix = config.prefix match {
      case Some(p) if p.endsWith(".") || p.isEmpty => p
      case Some(p)                                 => s"$p."
      case None                                    => Config.DefaultStatsDPrefix
    }
    s"${prefix}${metric.key}:${metric.value}|${metric.`type`}|#$tagStr"
  }
}
