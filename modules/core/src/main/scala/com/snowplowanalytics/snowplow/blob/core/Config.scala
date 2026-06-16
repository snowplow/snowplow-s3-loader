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

import java.net.URI

import scala.concurrent.duration.FiniteDuration

import com.comcast.ip4s.Port

import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._

import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, Metrics => CommonMetrics, Sentry}
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig

case class Config[+Factory, +Source, +BadSink](
  license: AcceptedLicense,
  input: Source,
  output: Config.Output[BadSink],
  streams: Factory,
  purpose: Config.Purpose,
  batching: Config.Batching,
  cpuParallelismFactor: BigDecimal,
  uploadParallelismFactor: BigDecimal,
  initialBufferSize: Option[Int],
  decompression: DecompressionConfig,
  monitoring: Config.Monitoring
)

object Config {

  case class Output[+BadSink](
    good: BlobSink,
    bad: SinkWithMetadata[BadSink]
  )

  case class BlobSink(
    path: URI,
    partitionFormat: Option[String],
    filenamePrefix: Option[String],
    compressionType: CompressionType
  )

  sealed trait CompressionType
  object CompressionType {
    case object Gzip extends CompressionType
  }

  case class SinkWithMetadata[+BadSink](
    sink: BadSink,
    maxRecordSize: Int
  )

  case class SinkMetadata(
    maxRecordSize: Int
  )

  sealed trait Purpose
  object Purpose {
    case object Enriched extends Purpose
    case object SDJ extends Purpose
  }

  case class Batching(
    maxBytes: Long,
    maxDelay: FiniteDuration
  )

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry.Config],
    healthProbe: HealthProbe
  )

  case class Metrics(
    statsd: Option[CommonMetrics.StatsdConfig],
    prometheus: CommonMetrics.PrometheusConfig
  )

  case class HealthProbe(port: Port, unhealthyLatency: FiniteDuration)

  implicit def decoder[Factory: Decoder, Source: Decoder, BadSink: Decoder]: Decoder[Config[Factory, Source, BadSink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val licenseDecoder =
      AcceptedLicense.decoder(AcceptedLicense.DocumentationLink("https://docs.snowplow.io/limited-use-license-1.1/"))
    implicit val sinkWithMetadataDecoder = for {
      sink <- Decoder[BadSink]
      metadata <- deriveConfiguredDecoder[SinkMetadata]
    } yield SinkWithMetadata(sink, metadata.maxRecordSize)
    implicit val compressionTypeDecoder: Decoder[CompressionType] =
      Decoder[String].emap {
        case compressionType if compressionType.toLowerCase == "gzip" => Right(CompressionType.Gzip)
        case _                                                        => Left(s"Only possible compression type: GZIP")
      }
    implicit val blobSinkDecoder = deriveConfiguredDecoder[BlobSink]
    implicit val outputDecoder   = deriveConfiguredDecoder[Output[BadSink]]
    implicit val purposeDecoder: Decoder[Purpose] =
      Decoder[String].emap {
        case purpose if purpose.toLowerCase == "enriched_events" => Right(Purpose.Enriched)
        case purpose if purpose.toLowerCase == "self_describing" => Right(Purpose.SDJ)
        case _ => Left(s"Purpose not supported. Possible values: ENRICHED_EVENTS, SELF_DESCRIBING")
      }
    implicit val batchingDecoder    = deriveConfiguredDecoder[Batching]
    implicit val sentryDecoder      = Sentry.ConfigM.sentryDecoder
    implicit val metricsDecoder     = deriveConfiguredDecoder[Metrics]
    implicit val healthProbeDecoder = deriveConfiguredDecoder[HealthProbe]
    implicit val monitoringDecoder  = deriveConfiguredDecoder[Monitoring]

    deriveConfiguredDecoder[Config[Factory, Source, BadSink]]
      .emap(DynamicPath.validatePartitionFormat)
  }
}
