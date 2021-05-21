package com.snowplowanalytics.s3.loader.monitoring

import java.time.{Duration, Instant}
import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.{ Future, ExecutionContext }

import org.slf4j.LoggerFactory

import com.snowplowanalytics.s3.loader.Config
import com.snowplowanalytics.s3.loader.processing.Batch.Meta

object StatsD {

  val CollectorLatencyName  = "latency.collector_to_s3.min"
  val CollectorTstampIdx = 3

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val EC: ExecutionContext =
    scala.concurrent.ExecutionContext.global

  final case class KVMetric(key: String, value: String)

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
    logger.debug("Sending metrics")

    val result = Future.traverse(fromMeta(meta)) { metric =>
      val strMetric = statsDFormat(config, metric).getBytes(UTF_8)

      Future {
        val socket = new DatagramSocket
        val ip = InetAddress.getByName(config.hostname)
        socket.send(new DatagramPacket(strMetric, strMetric.length, ip, config.port))
      }
    }

    result.map(_ => ())
  }

  /** Build a list of metrics from metadata */
  def fromMeta(meta: Meta): List[KVMetric] =
    meta.earliestTstamp.map(getTstampMetrics).toList

  /** Build a metric with difference between a timestamp and current time */
  def getTstampMetrics(collectorTstamp: Instant): KVMetric = {
    val diff = Duration.between(collectorTstamp, Instant.now())
    KVMetric(CollectorLatencyName, diff.toSeconds.toString)
  }

  private def statsDFormat(config: Config.StatsD, metric: KVMetric): String = {
    val tagStr = config.tags.map { case (k, v) => s"$k:$v" }.mkString(",")
    val prefix = config.prefix match {
      case Some(p) if p.endsWith(".") => p
      case Some(p) => s"$p."
      case None => ""
    }
    s"${prefix}${metric.key}:${metric.value}|g|#$tagStr"
  }
}
