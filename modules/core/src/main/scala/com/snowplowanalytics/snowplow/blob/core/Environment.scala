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

import cats.implicits._

import cats.effect.kernel.{Async, Resource, Sync}

import io.sentry.Sentry

import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}

import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, HealthProbe}

import com.snowplowanalytics.snowplow.streams.{Factory, Sink, SourceAndAck}

/**
 * Resources and runtime-derived configuration needed for processing events
 *
 * @param cpuParallelism
 *   The processing Pipe involves several steps, some of which are cpu-intensive. We run
 *   cpu-intensive steps in parallel, so that on big instances we can take advantage of all cores.
 *   For each of those cpu-intensive steps, `cpuParallelism` controls the parallelism of that step.
 * @param uploadParallelism
 *   Uploading steps aren't cpu-intensive but running them in parallel helps to use CPU better.
 *   Since it isn't cpu-intensive step, we want to control its parallelism differently then
 *   cpu-intensive steps which are controlled with `cpuParallelism`.
 * @param initialBufferSize
 *   Initial size of the buffer that holds the compressed events in-memory before they get written
 *   to a file. If the buffer is full and we want to keep adding events to it, a new bigger buffer
 *   is allocated and data get copied.
 */
case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  appHealth: AppHealth.Interface[F, String, RuntimeService],
  blobSink: BlobSink[F],
  badSink: Sink[F],
  metrics: Metrics[F],
  purpose: Config.Purpose,
  batching: Config.Batching,
  blobStorageConfig: Config.BlobSink,
  cpuParallelism: Int,
  uploadParallelism: Int,
  badSinkMaxSize: Int,
  initialBufferSize: Int
) {
  def badRowProcessor = BadRowProcessor(appInfo.name, appInfo.version)
}

object Environment {

  def fromConfig[F[_]: Async, FactoryConfig, SourceConfig, BadSinkConfig](
    config: Config[FactoryConfig, SourceConfig, BadSinkConfig],
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, BadSinkConfig]],
    toBlobSink: Config.BlobSink => Resource[F, BlobSink[F]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- enableSentry[F](appInfo, config.monitoring.sentry)
      factory <- toFactory(config.streams)
      sourceAndAck <- factory.source(config.input)
      sourceReporter = sourceAndAck.isHealthy(config.monitoring.healthProbe.unhealthyLatency).map(_.showIfUnhealthy)
      appHealth <- Resource.eval(AppHealth.init[F, String, RuntimeService](List(sourceReporter)))
      _ <- HealthProbe.resource(config.monitoring.healthProbe.port, appHealth)
      blobSink <- toBlobSink(config.output.good)
      badSink <- factory.sink(config.output.bad.sink).onError { case _ =>
                   Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink))
                 }
      metrics <- Resource.eval(Metrics.build(config.monitoring.metrics))
      cpuParallelism    = chooseCpuParallelism(config)
      uploadParallelism = chooseUploadParallelism(config)
      initialBufferSize = chooseInitialBufferSize(config.purpose, config.initialBufferSize, config.batching.maxBytes)
      _ <- Resource.eval(appHealth.beHealthyForSetup)
    } yield Environment(
      appInfo           = appInfo,
      source            = sourceAndAck,
      appHealth         = appHealth,
      blobSink          = blobSink,
      badSink           = badSink,
      metrics           = metrics,
      purpose           = config.purpose,
      batching          = config.batching,
      blobStorageConfig = config.output.good,
      cpuParallelism    = cpuParallelism,
      uploadParallelism = uploadParallelism,
      badSinkMaxSize    = config.output.bad.maxRecordSize,
      initialBufferSize = initialBufferSize
    )

  private def enableSentry[F[_]: Sync](appInfo: AppInfo, config: Option[Config.Sentry]): Resource[F, Unit] =
    config match {
      case Some(c) =>
        val acquire = Sync[F].delay {
          Sentry.init { options =>
            options.setDsn(c.dsn)
            options.setRelease(appInfo.version)
            c.tags.foreach { case (k, v) =>
              options.setTag(k, v)
            }
          }
        }

        Resource.makeCase(acquire) {
          case (_, Resource.ExitCase.Errored(e)) => Sync[F].delay(Sentry.captureException(e)).void
          case _                                 => Sync[F].unit
        }
      case None =>
        Resource.unit[F]
    }

  /**
   * See the description of `cpuParallelism` on the [[Environment]] class
   *
   * For bigger instances (more cores) we want more parallelism, so that cpu-intensive steps can
   * take advantage of all the cores.
   */
  private def chooseCpuParallelism(config: AnyConfig): Int =
    (Runtime.getRuntime.availableProcessors * config.cpuParallelismFactor)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

  /**
   * See the description of `uploadParallelism` on the [[Environment]] class
   */
  private def chooseUploadParallelism(config: AnyConfig): Int =
    (Runtime.getRuntime.availableProcessors * config.uploadParallelismFactor)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

  /**
   * See the description of `initialBufferSize` on the [[Environment]] class
   */
  private def chooseInitialBufferSize(
    purpose: Config.Purpose,
    initialBufferSize: Option[Int],
    maxBytes: Long
  ): Int =
    initialBufferSize.getOrElse {
      if (purpose == Config.Purpose.Enriched) (maxBytes * 1.1).toInt
      else (maxBytes / 8).toInt
    }
}
