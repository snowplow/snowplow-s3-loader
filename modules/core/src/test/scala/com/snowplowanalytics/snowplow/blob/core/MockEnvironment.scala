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

import java.io.ByteArrayInputStream
import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream

import scala.concurrent.duration._

import cats.implicits._

import cats.effect.IO
import cats.effect.kernel.{Ref, Unique}

import fs2.Stream

import io.circe.parser

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.iglu.core.SelfDescribingData

import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo}
import com.snowplowanalytics.snowplow.streams.{
  EventProcessingConfig,
  EventProcessor,
  ListOfList,
  Sink,
  Sinkable,
  SourceAndAck,
  TokenedEvents
}
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    /* Output events */
    case class WroteFile(path: String, uncompressed: String) extends Action
    case class SentToBad(bad: List[BadRow]) extends Action
    /* Metrics */
    case class AddedCountMetric(count: Long) extends Action
    case class SetLatencyMetric(latency: FiniteDuration) extends Action
    case class SetE2ELatencyMetric(e2eLatency: FiniteDuration) extends Action
  }
  import Action._

  def build(
    input: Stream[IO, TokenedEvents],
    blobStorageConfig: Config.BlobSink,
    purpose: Config.Purpose,
    maxBytes: Long           = 64 * 1024 * 1024,
    maxDelay: FiniteDuration = 2.minutes
  ): IO[MockEnvironment] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      env = Environment(
              appInfo   = appInfo,
              source    = testSourceAndAck(input, state),
              appHealth = testAppHealth,
              blobSink  = testBlobSink(state),
              badSink   = testBadSink(state),
              metrics   = testMetrics(state),
              purpose   = purpose,
              batching = Config.Batching(
                maxBytes = maxBytes,
                maxDelay = maxDelay
              ),
              blobStorageConfig   = blobStorageConfig,
              cpuParallelism      = 2,
              uploadParallelism   = 1,
              badSinkMaxSize      = Int.MaxValue,
              initialBufferSize   = 8 * 1024 * 1024,
              decompressionConfig = DecompressionConfig(maxBytesInBatch = 5242880, maxBytesSinglePayload = 10000000)
            )
    } yield MockEnvironment(state, env)

  val appInfo = new AppInfo {
    def name        = "blob-loader"
    def version     = "0.0.0"
    def dockerAlias = s"snowplow/$name:$version"
    def cloud       = "OnPrem"
  }

  private def testSourceAndAck(input: Stream[IO, TokenedEvents], state: Ref[IO, Vector[Action]]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig[IO], processor: EventProcessor[IO]): Stream[IO, Nothing] =
        input
          .through(processor)
          .chunks
          .evalMap { tokens =>
            state.update(_ :+ Checkpointed(tokens.toList))
          }
          .drain

      override def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)

      def currentStreamLatency: IO[Option[FiniteDuration]] =
        IO.pure(None)
    }

  private def testAppHealth: AppHealth.Interface[IO, String, RuntimeService] =
    new AppHealth.Interface[IO, String, RuntimeService] {
      def beHealthyForSetup: IO[Unit]                                     = IO.unit
      def beUnhealthyForSetup(alert: String): IO[Unit]                    = IO.unit
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit]   = IO.unit
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] = IO.unit
    }

  private def testBlobSink(ref: Ref[IO, Vector[Action]]) =
    new BlobSink[IO] {
      val uuidRegex = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
      override def write(uri: URI, events: ByteBuffer): IO[Unit] =
        ref.update(_ :+ WroteFile(uri.toString.replaceAll(uuidRegex, "xxxx"), decompress(events.slice)))
    }

  private def testBadSink(ref: Ref[IO, Vector[Action]]): Sink[IO] =
    new Sink[IO] {
      override def sink(batch: ListOfList[Sinkable]): IO[Unit] =
        for {
          parsed <- batch.asIterable.toList.traverse(parseBad)
          _ <- ref.update(_ :+ SentToBad(parsed))
        } yield ()

      override def isHealthy: IO[Boolean] = IO.pure(true)
    }

  private def parseBad(sinkable: Sinkable): IO[BadRow] =
    for {
      str <- IO(new String(sinkable.bytes, StandardCharsets.UTF_8))
      json <- parser.parse(str) match {
                case Right(j)  => IO.pure(j)
                case Left(err) => IO.raiseError(new RuntimeException(s"Can't parse bad row: $err"))
              }
      bad <- json.as[SelfDescribingData[BadRow]] match {
               case Right(sdd) => IO.pure(sdd.data)
               case Left(err)  => IO.raiseError(new RuntimeException(s"Can't decode bad row: $err"))
             }
    } yield bad

  private def testMetrics(ref: Ref[IO, Vector[Action]]): Metrics[IO] =
    new Metrics[IO] {
      def addCount(count: Long): IO[Unit] =
        ref.update(_ :+ AddedCountMetric(count))

      def setLatency(latency: FiniteDuration): IO[Unit] =
        ref.update(_ :+ SetLatencyMetric(latency))

      def setE2ELatency(e2eLatency: FiniteDuration): IO[Unit] =
        ref.update(_ :+ SetE2ELatencyMetric(e2eLatency))

      def scrape: IO[String] = IO.pure("")

      def report: Stream[IO, Nothing] = Stream.never[IO]
    }

  private def decompress(byteBuffer: ByteBuffer): String = {
    val bais         = new ByteArrayInputStream(byteBuffer.array, byteBuffer.position, byteBuffer.limit);
    val gis          = new GZIPInputStream(bais)
    val uncompressed = gis.readAllBytes
    new String(uncompressed, StandardCharsets.UTF_8)
  }
}
