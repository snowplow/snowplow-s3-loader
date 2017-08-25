/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd.
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

package com.snowplowanalytics.snowplow.storage.kinesis.s3

// Scalaz
import scalaz._
import Scalaz._

// Java
import java.text.SimpleDateFormat

// Scala
import scala.util.Try

package model {

  case class AWSConfig(accessKey: String, secretKey: String)
  case class NSQConfig(
    channelName: String,
    host: String,
    port: Int,
    lookupPort: Int
  )
  case class KinesisConfig(
    initialPosition: String,
    initialTimestamp: Option[String],
    maxRecords: Long,
    region: String,
    appName: String
  ) {
    val timestamp = initialTimestamp
      .toRight("An initial timestamp needs to be provided when choosing AT_TIMESTAMP")
      .right.flatMap { s =>
        val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
        utils.fold(Try(format.parse(s)))(t => Left(t.getMessage), Right(_))
      }
    require(initialPosition != "AT_TIMESTAMP" || timestamp.isRight, timestamp.left.getOrElse(""))

    val endpoint = region match {
      case "cn-north-1" => "kinesis.cn-north-1.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    }
  }
  case class BufferConfig(byteLimit: Long, recordLimit: Long, timeLimit: Long)
  case class StreamsConfig(
    streamNameIn: String,
    streamNameOut: String,
    buffer: BufferConfig
  )
  case class S3Config(
    region: String,
    bucket: String,
    format: String,
    maxTimeout: Long
  ) {
    val endpoint = region match {
      case "us-east-1" => "https://s3.amazonaws.com"
      case "cn-north-1" => "https://s3.cn-north-1.amazonaws.com.cn"
      case _ => s"https://s3-$region.amazonaws.com"
    }
  }
  case class LoggingConfig(level: String)
  case class MonitoringConfig(
    collectorUri: String,
    collectorPort: Int,
    appId: String,
    method: String
  )
  case class S3LoaderConfig(
    source: String,
    sink: String,
    aws: AWSConfig,
    nsq: NSQConfig,
    kinesis: KinesisConfig,
    streams: StreamsConfig,
    s3: S3Config,
    logging: LoggingConfig,
    monitoring: Option[MonitoringConfig]
  )
}
