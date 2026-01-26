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
package com.snowplowanalytics.snowplow.blob.aws

import java.nio.file.Paths
import java.net.URI

import scala.concurrent.duration.DurationInt

import org.specs2.Specification

import cats.Id

import cats.effect.{ExitCode, IO}

import cats.effect.testing.specs2.CatsEffect

import com.comcast.ip4s.Port

import com.snowplowanalytics.snowplow.runtime.Metrics.StatsdConfig
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, ConfigParser}

import com.snowplowanalytics.snowplow.streams.kinesis.{
  BackoffPolicy,
  KinesisHttpSourceConfig,
  KinesisSinkConfig,
  KinesisSinkConfigM,
  KinesisSourceConfig
}

import com.snowplowanalytics.snowplow.blob.core.Config

class AwsConfigSpec extends Specification with CatsEffect {

  def is = s2"""
  Config parse should be able to parse
    minimal aws config $minimal
    reference aws config $reference
  """

  private def minimal =
    assert(
      resource = "/config.aws.minimal.hocon",
      expectedResult = Right(
        AwsConfigSpec.minimalConfig
      )
    )

  private def reference =
    assert(
      resource = "/config.aws.reference.hocon",
      expectedResult = Right(
        AwsConfigSpec.referenceConfig
      )
    )

  private def assert(
    resource: String,
    expectedResult: Either[ExitCode, Config[EmptyConfig, KinesisHttpSourceConfig, KinesisSinkConfig]]
  ) = {
    val path = Paths.get(getClass.getResource(resource).toURI)
    ConfigParser.configFromFile[IO, Config[EmptyConfig, KinesisHttpSourceConfig, KinesisSinkConfig]](path).value.map { result =>
      result must beEqualTo(expectedResult)
    }
  }
}

object AwsConfigSpec {
  private val minimalConfig = Config[EmptyConfig, KinesisHttpSourceConfig, KinesisSinkConfig](
    license = AcceptedLicense(),
    input = KinesisHttpSourceConfig(
      kinesis = KinesisSourceConfig(
        appName                          = "snowplow-s3-loader",
        streamName                       = "snowplow-enriched",
        workerIdentifier                 = "testWorkerId",
        initialPosition                  = KinesisSourceConfig.InitialPosition.Latest,
        retrievalMode                    = KinesisSourceConfig.Retrieval.Polling(750, 1500.millis),
        customEndpoint                   = None,
        dynamodbCustomEndpoint           = None,
        cloudwatchCustomEndpoint         = None,
        leaseDuration                    = 10.seconds,
        maxLeasesToStealAtOneTimeFactor  = BigDecimal(2),
        checkpointThrottledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
        debounceCheckpoints              = 10.seconds,
        maxRetries                       = 10,
        apiCallAttemptTimeout            = 15.seconds
      ),
      http = None
    ),
    output = Config.Output(
      good = Config.BlobSink(
        path            = URI.create("s3://snowplow-enriched/"),
        partitionFormat = None,
        filenamePrefix  = None,
        compressionType = Config.CompressionType.Gzip
      ),
      bad = Config.SinkWithMetadata(
        sink = KinesisSinkConfigM[Id](
          streamName             = "snowplow-bad",
          throttledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
          recordLimit            = 500,
          byteLimit              = 5242880,
          customEndpoint         = None,
          maxRetries             = 10
        ),
        maxRecordSize = 1000000
      )
    ),
    streams = EmptyConfig(),
    purpose = Config.Purpose.Enriched,
    batching = Config.Batching(
      maxBytes = 67108864,
      maxDelay = 2.minutes
    ),
    cpuParallelismFactor    = BigDecimal(1),
    uploadParallelismFactor = BigDecimal(2),
    initialBufferSize       = None,
    monitoring = Config.Monitoring(
      metrics     = Config.Metrics(None),
      sentry      = None,
      healthProbe = Config.HealthProbe(port = Port.fromInt(8000).get, unhealthyLatency = 2.minutes)
    )
  )

  private val referenceConfig = Config[EmptyConfig, KinesisHttpSourceConfig, KinesisSinkConfig](
    license = AcceptedLicense(),
    input = KinesisHttpSourceConfig(
      kinesis = KinesisSourceConfig(
        appName                          = "snowplow-s3-loader",
        streamName                       = "snowplow-sdjs",
        workerIdentifier                 = "testWorkerId",
        initialPosition                  = KinesisSourceConfig.InitialPosition.TrimHorizon,
        retrievalMode                    = KinesisSourceConfig.Retrieval.Polling(750, 1500.millis),
        customEndpoint                   = None,
        dynamodbCustomEndpoint           = None,
        cloudwatchCustomEndpoint         = None,
        leaseDuration                    = 10.seconds,
        maxLeasesToStealAtOneTimeFactor  = BigDecimal(2),
        checkpointThrottledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
        debounceCheckpoints              = 10.seconds,
        maxRetries                       = 10,
        apiCallAttemptTimeout            = 15.seconds
      ),
      http = None
    ),
    output = Config.Output(
      good = Config.BlobSink(
        path            = URI.create("s3://snowplow-events/"),
        partitionFormat = Some("{vendor}.{schema}/model={model}/date={yyyy}-{MM}-{dd}/time={HH}{mm}{ss}"),
        filenamePrefix  = Some("pre-"),
        compressionType = Config.CompressionType.Gzip
      ),
      bad = Config.SinkWithMetadata(
        sink = KinesisSinkConfigM[Id](
          streamName             = "snowplow-bad",
          throttledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
          recordLimit            = 500,
          byteLimit              = 5242880,
          customEndpoint         = None,
          maxRetries             = 10
        ),
        maxRecordSize = 1000000
      )
    ),
    streams = EmptyConfig(),
    purpose = Config.Purpose.SDJ,
    batching = Config.Batching(
      maxBytes = 67108864,
      maxDelay = 1.minute
    ),
    cpuParallelismFactor    = BigDecimal(1),
    uploadParallelismFactor = BigDecimal(2),
    initialBufferSize       = Some(70000000),
    monitoring = Config.Monitoring(
      metrics = Config.Metrics(
        statsd = Some(
          StatsdConfig(
            hostname = "127.0.0.1",
            port     = 8125,
            tags     = Map("env" -> "prod"),
            period   = 1.minute,
            prefix   = "snowplow.blob.loader.aws"
          )
        )
      ),
      sentry = Some(Config.SentryM[Id](dsn = "https://public@sentry.example.com/1", tags = Map("myTag" -> "xyz"))),
      healthProbe = Config.HealthProbe(
        port             = Port.fromInt(8000).get,
        unhealthyLatency = 2.minutes
      )
    )
  )
}
