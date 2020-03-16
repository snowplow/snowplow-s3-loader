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

// Java
import java.text.SimpleDateFormat

// Scala
import scala.util.Try

import java.util.Properties
package model {

  case class AWSConfig(accessKey: String, secretKey: String)
  case class NSQConfig(
    channelName: String,
    host: String,
    port: Int,
    lookupPort: Int
  )
  case class KafkaConfig(
    brokers: String,
    appName: String,
    pollTime: Option[Long],
    startFromBeginning: Boolean
  ) {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", brokers)
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    properties.setProperty("enable.auto.commit", "false")

    properties.setProperty("group.id", appName)
  }
  case class KinesisConfig(
    initialPosition: String,
    initialTimestamp: Option[String],
    maxRecords: Long,
    region: String,
    appName: String,
    customEndpoint: Option[String]
  ) {
    val timestampEither = initialTimestamp
      .toRight("An initial timestamp needs to be provided when choosing AT_TIMESTAMP")
      .right.flatMap { s =>
        val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
        utils.fold(Try(format.parse(s)))(t => Left(t.getMessage), Right(_))
      }
    require(initialPosition != "AT_TIMESTAMP" || timestampEither.isRight, timestampEither.left.getOrElse(""))
    val timestamp = timestampEither.right.toOption

    val endpoint = customEndpoint.getOrElse(region match {
      case "cn-north-1" => "kinesis.cn-north-1.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    })
  }
  case class BufferConfig(byteLimit: Long, recordLimit: Long, timeLimit: Long)
  case class StreamsConfig(
    inStreamName: String,
    outStreamName: String,
    buffer: BufferConfig
  )
  case class S3Config(
    region: String,
    bucket: String,
    partitionedBucket: Option[String],
    format: String,
    maxTimeout: Long,
    outputDirectory: Option[String],
    customEndpoint: Option[String],
    dateFormat: Option[String] = None,
    filenamePrefix: Option[String] = None
  ) {
    val endpoint = customEndpoint.getOrElse(region match {
      case "us-east-1" => "https://s3.amazonaws.com"
      case "cn-north-1" => "https://s3.cn-north-1.amazonaws.com.cn"
      case _ => s"https://s3-$region.amazonaws.com"
    })
  }
  case class LoggingConfig(level: String)
  case class SnowplowMonitoringConfig(
    collectorUri: String,
    collectorPort: Int,
    appId: String,
    method: String
  )
  case class MonitoringConfig(snowplow: SnowplowMonitoringConfig)
  case class S3LoaderConfig(
    source: String,
    sink: String,
    aws: AWSConfig,
    nsq: NSQConfig,
    kafka: KafkaConfig,
    kinesis: KinesisConfig,
    streams: StreamsConfig,
    s3: S3Config,
    monitoring: Option[MonitoringConfig]
  )
}
