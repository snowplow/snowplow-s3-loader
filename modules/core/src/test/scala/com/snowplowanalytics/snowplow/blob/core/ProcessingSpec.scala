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
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Base64, UUID}

import scala.concurrent.duration._

import cats.data.NonEmptyList

import cats.effect.IO
import cats.effect.kernel.Unique

import fs2.{Chunk, Stream}

import org.specs2.Specification

import cats.effect.testkit.TestControl

import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.github.luben.zstd.ZstdOutputStream

import com.snowplowanalytics.snowplow.streams.TokenedEvents
import com.snowplowanalytics.snowplow.streams.compression.{Compressor, GzipCompressor, ZstdCompressor}

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload, Processor}

import MockEnvironment._

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = s2"""
  Blob loader should
    write enriched events to blob storage                          $e1
    write SDJs grouped by schema to blob storage and emit bad rows $e2
    batch events up to maxBytes limit                              $e3
    emit batches after maxDelay timeout                            $e4
    decompress and write zstd-compressed enriched events           $e5
    emit bad row for corrupt zstd-compressed input                 $e6
    decompress and write gzip-compressed enriched events           $e7
    decompress and write mixed plain + zstd + gzip events          $e8
  """

  // Write enriched events to blob storage
  def e1 = TestControl.executeEmbed {
    val token = new Unique.Token
    val input = Stream.emit(TokenedEvents(Chunk.from(List.fill(2)(enriched.toTsv.toByteBuffer)), token))

    for {
      control <- MockEnvironment.build(input, blobStorageConfig, Config.Purpose.Enriched)
      _ <- Processing.stream[IO](control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteFile(enrichedPath, s"${enriched.toTsv}\n${enriched.toTsv}\n"),
        Action.AddedCountMetric(2),
        Action.SetE2ELatencyMetric(0.microseconds),
        Action.Checkpointed(List(token))
      )
    )
  }

  // Write SDJs grouped by schema to blob storage and emit bad rows $e2
  def e2 = TestControl.executeEmbed {
    val Token  = new Unique.Token
    val events = List(sdj1.toByteBuffer, sdj2.toByteBuffer, invalidSdj.toByteBuffer, sdj1.toByteBuffer, sdj2.toByteBuffer)
    val input  = Stream.emit(TokenedEvents(Chunk.from(events), Token))

    for {
      control <- MockEnvironment.build(input, blobStorageConfigWithPartitioning, Config.Purpose.SDJ)
      _ <- Processing.stream[IO](control.environment).compile.drain
      state <- control.state.get
    } yield state should beLike {
      case Vector(
            w1: Action.WroteFile,
            w2: Action.WroteFile,
            Action.AddedCountMetric(4),
            Action.SentToBad(List(GenericError)),
            Action.Checkpointed(List(Token))
          ) =>
        List(w1, w2) must containTheSameElementsAs(
          List(Action.WroteFile(sdj1Path, s"$sdj1\n$sdj1\n"), Action.WroteFile(sdj2Path, s"$sdj2\n$sdj2\n"))
        )
    }
  }

  // Batch events up to maxBytes limit
  // After writing 8 records to the compressed stream its size exceeds 114 compressed bytes
  def e3 = TestControl.executeEmbed {
    val (token1, token2, token3) = (new Unique.Token, new Unique.Token, new Unique.Token)
    val input = Stream.emits(
      List(
        TokenedEvents(Chunk.from(List.fill(4)(enriched.toTsv.toByteBuffer)), token1),
        TokenedEvents(Chunk.from(List.fill(4)(enriched.toTsv.toByteBuffer)), token2),
        TokenedEvents(Chunk.from(List.fill(4)(enriched.toTsv.toByteBuffer)), token3)
      )
    )

    for {
      control <- MockEnvironment.build(input, blobStorageConfig, Config.Purpose.Enriched, maxBytes = 114)
      _ <- Processing.stream[IO](control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteFile(enrichedPath, s"${enriched.toTsv}\n" * 8),
        Action.AddedCountMetric(8),
        Action.SetE2ELatencyMetric(0.microseconds),
        Action.Checkpointed(List(token1, token2)),
        Action.WroteFile(enrichedPath, s"${enriched.toTsv}\n" * 4),
        Action.AddedCountMetric(4),
        Action.SetE2ELatencyMetric(0.microseconds),
        Action.Checkpointed(List(token3))
      )
    )
  }

  // Emit batches after maxDelay timeout
  def e4 = TestControl.executeEmbed {
    val (token1, token2) = (new Unique.Token, new Unique.Token)
    val input = Stream
      .emits(
        List(
          TokenedEvents(Chunk.from(List.fill(2)(enriched.toTsv.toByteBuffer)), token1),
          TokenedEvents(Chunk.from(List.fill(2)(enriched.toTsv.toByteBuffer)), token2)
        )
      )
      .covary[IO]
      .spaced(3.seconds)

    for {
      control <- MockEnvironment.build(input, blobStorageConfig, Config.Purpose.Enriched, maxDelay = 2.seconds)
      _ <- Processing.stream[IO](control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteFile("blob://path/1970-01-01-000002-xxxx.gz", s"${enriched.toTsv}\n${enriched.toTsv}\n"),
        Action.AddedCountMetric(2),
        Action.SetE2ELatencyMetric(2.seconds),
        Action.Checkpointed(List(token1)),
        Action.WroteFile("blob://path/1970-01-01-000005-xxxx.gz", s"${enriched.toTsv}\n${enriched.toTsv}\n"),
        Action.AddedCountMetric(2),
        Action.SetE2ELatencyMetric(5.seconds),
        Action.Checkpointed(List(token2))
      )
    )
  }
  // Decompress and write zstd-compressed enriched events
  def e5 = TestControl.executeEmbed {
    val (token1, token2) = (new Unique.Token, new Unique.Token)
    val input = Stream.emits(
      List(
        TokenedEvents(Chunk(compress(zstdFactory, List(enriched.toTsv, enriched.toTsv))), token1),
        TokenedEvents(Chunk(compress(zstdFactory, List(enriched.toTsv, enriched.toTsv))), token2)
      )
    )

    for {
      control <- MockEnvironment.build(input, blobStorageConfig, Config.Purpose.Enriched)
      _ <- Processing.stream[IO](control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteFile(enrichedPath, s"${enriched.toTsv}\n" * 4),
        Action.AddedCountMetric(4),
        Action.SetE2ELatencyMetric(0.microseconds),
        Action.Checkpointed(List(token1, token2))
      )
    )
  }

  // Emit bad row for corrupt zstd-compressed input
  def e6 = TestControl.executeEmbed {
    val token = new Unique.Token
    val input = Stream.emit(TokenedEvents(Chunk(corruptZstdCompressed), token))

    for {
      control <- MockEnvironment.build(input, blobStorageConfig, Config.Purpose.Enriched)
      _ <- Processing.stream[IO](control.environment).compile.drain
      state <- control.state.get
    } yield state should beLike {
      case Vector(
            Action.AddedCountMetric(0),
            Action.SentToBad(List(_: BadRow.LoaderParsingError)),
            Action.Checkpointed(List(_))
          ) =>
        ok
    }
  }

  // Decompress and write gzip-compressed enriched events
  def e7 = TestControl.executeEmbed {
    val (token1, token2) = (new Unique.Token, new Unique.Token)
    val input = Stream.emits(
      List(
        TokenedEvents(Chunk(compress(gzipFactory, List(enriched.toTsv, enriched.toTsv))), token1),
        TokenedEvents(Chunk(compress(gzipFactory, List(enriched.toTsv, enriched.toTsv))), token2)
      )
    )

    for {
      control <- MockEnvironment.build(input, blobStorageConfig, Config.Purpose.Enriched)
      _ <- Processing.stream[IO](control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteFile(enrichedPath, s"${enriched.toTsv}\n" * 4),
        Action.AddedCountMetric(4),
        Action.SetE2ELatencyMetric(0.microseconds),
        Action.Checkpointed(List(token1, token2))
      )
    )
  }

  // Decompress and write mixed plain + zstd + gzip events in one TokenedEvents
  def e8 = TestControl.executeEmbed {
    val token = new Unique.Token
    val input = Stream.emit(
      TokenedEvents(
        Chunk(
          ByteBuffer.wrap(enriched.toTsv.getBytes(StandardCharsets.UTF_8)),
          compress(zstdFactory, List(enriched.toTsv)),
          compress(gzipFactory, List(enriched.toTsv))
        ),
        token
      )
    )

    for {
      control <- MockEnvironment.build(input, blobStorageConfig, Config.Purpose.Enriched)
      _ <- Processing.stream[IO](control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.WroteFile(enrichedPath, s"${enriched.toTsv}\n" * 3),
        Action.AddedCountMetric(3),
        Action.SetE2ELatencyMetric(0.microseconds),
        Action.Checkpointed(List(token))
      )
    )
  }
}

object ProcessingSpec {

  val zstdFactory = ZstdCompressor.factory(3)
  val gzipFactory = GzipCompressor.factory(6)

  def compress(factory: Compressor.Factory, events: List[String]): ByteBuffer = {
    val compressor = factory.buildAndInitialize(1000000, 1)
    events.foreach { e =>
      val bytes = e.getBytes(StandardCharsets.UTF_8)
      val _     = compressor.addRecord(bytes, 0, bytes.length)
    }
    compressor.result
  }

  def corruptZstdCompressed: ByteBuffer = {
    val baos = new java.io.ByteArrayOutputStream()
    val zstd = new ZstdOutputStream(baos)
    zstd.write(1) // Snowplow header: compression format version
    zstd.write(1) // Snowplow header: payload format version
    val sizeBytes = ByteBuffer.allocate(4)
    sizeBytes.order(java.nio.ByteOrder.BIG_ENDIAN)
    sizeBytes.putInt(10) // record size = 10, but only 3 bytes follow
    zstd.write(sizeBytes.array())
    zstd.write(Array[Byte](1, 2, 3))
    zstd.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  val blobStorageConfig = Config.BlobSink(
    URI.create("blob://path/"),
    None,
    None,
    Config.CompressionType.Gzip
  )

  val blobStorageConfigWithPartitioning =
    blobStorageConfig.copy(partitionFormat = Some("{vendor}.{schema}"))

  val enriched = Event.minimal(
    id              = UUID.randomUUID,
    collectorTstamp = Instant.EPOCH,
    vCollector      = "test-collector",
    vEtl            = "test"
  )
  val enrichedPath = "blob://path/1970-01-01-000000-xxxx.gz"

  val sdj1 =
    """{"schema":"iglu:com.example/event1/jsonschema/1-0-0","data":{"field":"value1"}}"""
  val sdj1Path = "blob://path/com.example.event1/1970-01-01-000000-xxxx.gz"

  val sdj2 =
    """{"schema":"iglu:com.example/event2/jsonschema/1-0-0","data":{"field":"value2"}}"""
  val sdj2Path = "blob://path/com.example.event2/1970-01-01-000000-xxxx.gz"

  val invalidSdj = """{"data":{"field":"value"}}"""

  val GenericError = BadRow.GenericError(
    Processor(MockEnvironment.appInfo.name, MockEnvironment.appInfo.version),
    Failure.GenericFailure(Instant.EPOCH, NonEmptyList.one("Can't extract schema from self-describing event: INVALID_DATA_PAYLOAD")),
    Payload.RawPayload(new String(Base64.getEncoder.encode(invalidSdj.getBytes)))
  )

  implicit class RichString(s: String) {
    def toByteBuffer = {
      val bytes = s.getBytes(StandardCharsets.UTF_8)
      ByteBuffer.wrap(bytes)
    }
  }
}
