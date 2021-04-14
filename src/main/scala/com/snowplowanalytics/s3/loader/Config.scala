/**
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd.
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

case class Config(source: Source,
                  sink: Sink,
                  aws: AWS,
                  kinesis: Kinesis,
                  streams: StreamsConfig,
                  s3: S3,
                  monitoring: Option[Monitoring])

object Config {

  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

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
    def toKCL: InitialPositionInStream = this match {
      case InitialPosition.AtTimestamp(_) => InitialPositionInStream.AT_TIMESTAMP
      case InitialPosition.TrimHorizon => InitialPositionInStream.TRIM_HORIZON
      case InitialPosition.Latest => InitialPositionInStream.LATEST
    }
  }
  object InitialPosition {
    case class AtTimestamp(tstamp: Date) extends InitialPosition
    case object TrimHorizon extends InitialPosition
    case object Latest extends InitialPosition

    implicit val dateDecoder: Decoder[Date] =
      Decoder[Instant].map(Date.from)

    private val atTimestampDecoder: Decoder[InitialPosition] =
      Decoder.decodeJsonObject.emap { obj =>
        val root = obj.toMap
        val opt = for {
          atTimestamp <- root.get("AT_TIMESTAMP")
          atTimestampObj <- atTimestamp.asObject.map(_.toMap)
          timestampStr <- atTimestampObj.get("timestamp")
          timestamp <- timestampStr.as[Date].toOption
        } yield AtTimestamp(timestamp)
        opt match {
          case Some(atTimestamp) =>  atTimestamp.asRight
          case None => "Initial position can be either LATEST or TRIM_HORIZON string or AT_TIMESTAMP object (e.g. 2020-06-03T00:00:00Z)".asLeft
        }
      }

    implicit val initPositionDecoder: Decoder[InitialPosition] =
      Decoder[String].map(_.toLowerCase).emap {
        case "latest" => Latest.asRight
        case "trim_horizon" => TrimHorizon.asRight
        case other => s"Initial position $other cannot be decoded".asLeft
      }.or(atTimestampDecoder)
  }

  sealed trait Format
  object Format {
    case object Lzo extends Format
    case object Gzip extends Format

    implicit val formatDecoder: Decoder[Format] =
      Decoder[String].map(_.toLowerCase).emap {
        case "lzo" => Lzo.asRight
        case "gzip" => Gzip.asRight
        case other => s"Cannot parse $other into supported format".asLeft
      }
  }

  sealed trait Source
  object Source {
    case object Kinesis extends Source

    implicit val sourceDecoder: Decoder[Source] =
      Decoder[String].map(_.toLowerCase).emap {
        case "kinesis" => Kinesis.asRight
        case other => s"Cannot parse $other into supported source".asLeft
      }
  }

  sealed trait Sink
  object Sink {
    case object Kinesis extends Sink

    implicit val sinkDecoder: Decoder[Sink] =
      Decoder[String].map(_.toLowerCase).emap {
        case "kinesis" => Kinesis.asRight
        case other => s"Cannot parse $other into supported source".asLeft
      }
  }

  case class AWS(accessKey: String, secretKey: String)

  case class Kinesis(initialPosition: InitialPosition,
                     maxRecords: Long, region: String,
                     appName: String,
                     customEndpoint: Option[String],
                     disableCloudWatch: Option[Boolean]) {
    val endpoint = customEndpoint.getOrElse(region match {
      case "cn-north-1" => "kinesis.cn-north-1.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    })

    val disableCW = disableCloudWatch.getOrElse(false)
  }

  case class Buffer(byteLimit: Long,
                    recordLimit: Long,
                    timeLimit: Long)

  case class StreamsConfig(inStreamName: String,
                           outStreamName: String,
                           buffer: Buffer)

  case class S3(region: String,
                bucket: String,
                partitionedBucket: Option[String],
                format: Format,
                maxTimeout: Long,
                outputDirectory: Option[String],
                customEndpoint: Option[String],
                dateFormat: Option[String] = None,
                filenamePrefix: Option[String] = None) {
    val endpoint = customEndpoint.getOrElse(region match {
      case "us-east-1" => "https://s3.amazonaws.com"
      case "cn-north-1" => "https://s3.cn-north-1.amazonaws.com.cn"
      case _ => s"https://s3-$region.amazonaws.com"
    })
  }

  case class Logging(level: String)

  case class SnowplowMonitoring(collectorUri: String,
                                collectorPort: Int,
                                appId: String,
                                method: String)

  case class Monitoring(snowplow: SnowplowMonitoring)

  implicit def bufferConfigDecoder: Decoder[Buffer] =
    deriveDecoder[Buffer]

  implicit def streamsConfigDecoder: Decoder[StreamsConfig] =
    deriveDecoder[StreamsConfig]

  implicit def awsConfigDecoder: Decoder[AWS] =
    deriveDecoder[AWS]

  implicit def s3ConfigDecoder: Decoder[S3] =
    deriveDecoder[S3]

  implicit def kinesisConfigDecoder: Decoder[Kinesis] =
    deriveDecoder[Kinesis]

  implicit def loggingConfigDecoder: Decoder[Logging] =
    deriveDecoder[Logging]

  implicit def snowplowMonitoringConfigDecoder: Decoder[SnowplowMonitoring] =
    deriveDecoder[SnowplowMonitoring]

  implicit def monitoringConfigDecoder: Decoder[Monitoring] =
    deriveDecoder[Monitoring]

  implicit def configDecoder: Decoder[Config] =
    deriveDecoder[Config]

  implicit val configReader: ConfigReader[Config] =
    ConfigReader[Json].emap { json =>
      json.as[Config] match {
        case Left(error) => Left(ExceptionThrown(error))
        case Right(value) => Right(value)
      }
    }
}
