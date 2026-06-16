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

import java.nio.file.Paths
import java.net.URI

import scala.concurrent.duration.DurationInt

import org.specs2.Specification

import cats.effect.IO

import cats.effect.testing.specs2.CatsEffect

import com.comcast.ip4s.Port

import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.Decoder

import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, ConfigParser}
import com.snowplowanalytics.snowplow.runtime.Metrics.PrometheusConfig
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig

class ConfigSpec extends Specification with CatsEffect {

  def is = s2"""
  Config parsing should
    fail if partitioning is incompatible with purpose $invalidPartitioning
    set the default partitioning for SDJs $defaultPartitioningSDJs
  """

  private def invalidPartitioning =
    assert(
      resource = "/invalid_partitioning.hocon",
      expectedResult = Left(
        "Cannot resolve config: DecodingFailure at : Enriched events can only get partitioned by date and time"
      )
    )

  private def defaultPartitioningSDJs = {
    val expected = Config[EmptyConfig, EmptyConfig, EmptyConfig](
      license = AcceptedLicense(),
      input   = EmptyConfig(),
      output = Config.Output(
        good = Config.BlobSink(
          path            = URI.create("blob://path/"),
          partitionFormat = Some("{vendor}.{schema}"),
          filenamePrefix  = None,
          compressionType = Config.CompressionType.Gzip
        ),
        bad = Config.SinkWithMetadata(
          sink          = EmptyConfig(),
          maxRecordSize = 42
        )
      ),
      streams = EmptyConfig(),
      purpose = Config.Purpose.SDJ,
      batching = Config.Batching(
        maxBytes = 67108864,
        maxDelay = 2.minutes
      ),
      cpuParallelismFactor    = BigDecimal(1),
      uploadParallelismFactor = BigDecimal(2),
      initialBufferSize       = None,
      decompression           = DecompressionConfig(maxBytesInBatch = 5242880, maxBytesSinglePayload = 10000000),
      monitoring = Config.Monitoring(
        metrics     = Config.Metrics(statsd = None, prometheus = PrometheusConfig(tags = Map.empty)),
        sentry      = None,
        healthProbe = Config.HealthProbe(port = Port.fromInt(8000).get, unhealthyLatency = 2.minutes)
      )
    )

    assert(
      resource       = "/default_partitioning_sdjs.hocon",
      expectedResult = Right(expected)
    )
  }

  private def assert(
    resource: String,
    expectedResult: Either[String, Config[EmptyConfig, EmptyConfig, EmptyConfig]]
  ) = {
    val path = Paths.get(getClass.getResource(resource).toURI)
    ConfigParser.configFromFile[IO, Config[EmptyConfig, EmptyConfig, EmptyConfig]](path).value.map { result =>
      result must beEqualTo(expectedResult)
    }
  }
}

case class EmptyConfig()
object EmptyConfig {
  implicit val configuration: Configuration  = Configuration.default
  implicit val decoder: Decoder[EmptyConfig] = deriveConfiguredDecoder[EmptyConfig]
}
