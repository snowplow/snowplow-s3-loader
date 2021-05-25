/**
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.s3.loader

import java.time.Instant
import java.util.Date
import java.net.URI
import java.nio.file.Path

import cats.implicits._

import io.circe.{Decoder, Json}
import io.circe.generic.semiauto._

import com.typesafe.config.ConfigFactory

import pureconfig._
import pureconfig.error.ExceptionThrown
import pureconfig.module.circe._
import pureconfig.generic.ProductHint

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import com.snowplowanalytics.s3.loader.Config._

case class Config(region: Option[String], purpose: Purpose, input: Input, output: Output, buffer: Buffer, monitoring: Option[Monitoring])

object Config {

  val DefaultStatsDPrefix = "snowplow.s3loader"

  implicit def hint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def load(path: Path): Either[String, Config] =
    Either
      .catchNonFatal(ConfigFactory.parseFile(path.toFile).resolve())
      .leftMap(error => s"Failed to resolve config from $path,\n$error")
      .flatMap { config =>
        ConfigSource
          .fromConfig(config)
          .load[Config]
          .leftMap(_.toString)
      }

  sealed trait InitialPosition extends Product with Serializable {
    def toKCL: InitialPositionInStream =
      this match {
        case InitialPosition.AtTimestamp(_) =>
          InitialPositionInStream.AT_TIMESTAMP
        case InitialPosition.TrimHorizon => InitialPositionInStream.TRIM_HORIZON
        case InitialPosition.Latest      => InitialPositionInStream.LATEST
      }
  }
  object InitialPosition {
    final case class AtTimestamp(tstamp: Date) extends InitialPosition
    case object TrimHorizon extends InitialPosition
    case object Latest extends InitialPosition

    implicit val dateDecoder: Decoder[Date] =
      Decoder[Instant].map(Date.from)

    private val atTimestampDecoder: Decoder[InitialPosition] =
      Decoder.decodeJsonObject.emap { obj =>
        val root = obj.toMap
        val opt = for {
          atTimestamp <- root.collectFirst {
                           case (k, v) if k.toLowerCase == "at_timestamp" => v
                         }
          atTimestampObj <- atTimestamp.asObject.map(_.toMap)
          timestampStr <- atTimestampObj.get("timestamp")
          timestamp <- timestampStr.as[Date].toOption
        } yield AtTimestamp(timestamp)
        opt match {
          case Some(atTimestamp) => atTimestamp.asRight
          case None =>
            "Initial position can be either LATEST or TRIM_HORIZON string or AT_TIMESTAMP object (e.g. 2020-06-03T00:00:00Z)".asLeft
        }
      }

    implicit val initPositionDecoder: Decoder[InitialPosition] =
      Decoder[String]
        .map(_.toLowerCase)
        .emap {
          case "latest"       => Latest.asRight
          case "trim_horizon" => TrimHorizon.asRight
          case other          => s"Initial position $other cannot be decoded".asLeft
        }
        .or(atTimestampDecoder)
  }

  final case class Input(appName: String, streamName: String, position: InitialPosition, customEndpoint: Option[String], maxRecords: Int)

  sealed trait Purpose

  object Purpose {

    /** Data as a byte stream, without inspection or parsing */
    case object Raw extends Purpose

    /** Self-describing JSONs, such as Snowplow bad rows, allowing partitioning */
    case object SelfDescribingJson extends Purpose

    /** Snowplow Enriched events, resulting into enabled metrics */
    case object Enriched extends Purpose

    implicit def purposeDecoder: Decoder[Purpose] =
      Decoder[String].map(_.toLowerCase).emap {
        case "raw"             => Raw.asRight
        case "self_describing" => SelfDescribingJson.asRight
        case "enriched_events" => Enriched.asRight
        case other             => s"Cannot parse $other into supported output type".asLeft
      }
  }

  final case class S3Output(path: String,
                            dateFormat: Option[String],
                            filenamePrefix: Option[String],
                            compression: Compression,
                            maxTimeout: Int,
                            customEndpoint: Option[String]) {

    /** Just bucket name, without deeper path */
    def bucketName: String =
      withoutPrefix.split("/").head

    /** Base directory in the bucket */
    def outputDirectory: Option[String] = {
      val possiblyEmpty = withoutPrefix.split("/").tail.toList.mkString("/")
      if (possiblyEmpty.isEmpty) None else Some(possiblyEmpty)
    }

    // For backward-compatibility
    private val scheme = "s3://"
    private val withoutPrefix =
      if (path.startsWith(scheme)) path.drop(scheme.length) else path
  }

  final case class Output(s3: S3Output, badStreamName: String)

  sealed trait Compression
  object Compression {
    case object Lzo extends Compression
    case object Gzip extends Compression

    implicit def compressionDecoder: Decoder[Compression] =
      Decoder[String].map(_.toLowerCase).emap {
        case "lzo"  => Lzo.asRight
        case "gzip" => Gzip.asRight
        case other  => s"Cannot parse $other into supported format".asLeft
      }
  }

  final case class Buffer(byteLimit: Long, recordLimit: Long, timeLimit: Long)

  final case class Logging(level: String)

  final case class SnowplowMonitoring(collector: URI, appId: String)

  final case class StatsD(hostname: String, port: Int, tags: Map[String, String], prefix: Option[String])

  final case class Sentry(dsn: URI)

  /**
   * Different metrics services
   * @param cloudWatch embedded into Kinesis Connector lib, CWMetricsFactory
   *                   not recommended to use
   */
  case class Metrics(cloudWatch: Option[Boolean], statsd: Option[StatsD])

  /**
   * Monitoring configuration
   * @param snowplow Snowplow-powered monitoring with basic init/shutdown/failure tracking
   * @param metrics metrics services
   */
  case class Monitoring(snowplow: Option[SnowplowMonitoring], sentry: Option[Sentry], metrics: Option[Metrics])

  implicit def inputConfigDecoder: Decoder[Input] =
    deriveDecoder[Input]

  implicit def s3OutputConfigDecoder: Decoder[S3Output] =
    deriveDecoder[S3Output]

  implicit def outputConfigDecoder: Decoder[Output] =
    deriveDecoder[Output]

  implicit def bufferConfigDecoder: Decoder[Buffer] =
    deriveDecoder[Buffer]

  implicit def loggingConfigDecoder: Decoder[Logging] =
    deriveDecoder[Logging]

  implicit def metricsConfigDecoder: Decoder[Metrics] =
    deriveDecoder[Metrics]

  implicit def snowplowMonitoringConfigDecoder: Decoder[SnowplowMonitoring] =
    deriveDecoder[SnowplowMonitoring]

  implicit def statsdConfigDecoder: Decoder[StatsD] =
    deriveDecoder[StatsD]

  implicit def javaUriDecoder: Decoder[URI] =
    Decoder[String].emap(s =>
      Either
        .catchOnly[IllegalArgumentException](URI.create(s))
        .leftMap(_.getMessage))

  implicit def sentryConfigDecoder: Decoder[Sentry] =
    deriveDecoder[Sentry]

  implicit def monitoringConfigDecoder: Decoder[Monitoring] =
    deriveDecoder[Monitoring]

  implicit def configDecoder: Decoder[Config] =
    deriveDecoder[Config]

  implicit val configReader: ConfigReader[Config] =
    ConfigReader[Json].emap { json =>
      json.as[Config] match {
        case Left(error)  => Left(ExceptionThrown(error))
        case Right(value) => Right(value)
      }
    }
}
