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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Base64

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}

import io.circe.parser.parse

import cats.{Applicative, Foldable}
import cats.data.NonEmptyList
import cats.implicits._

import cats.effect.implicits._
import cats.effect.kernel.{Async, Clock, Sync, Unique}

import fs2.{Chunk, Pipe, Pull, Stream}

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload, Processor}

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.runtime.AppHealth
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, ListOfList, Sink, TokenedEvents}

object Processing {

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] =
    env.source
      .stream(EventProcessingConfig(EventProcessingConfig.NoWindowing, env.metrics.setLatency), eventProcessor(env))

  case class ParseResult(
    events: Map[SchemaKey, List[String]],
    parseFailures: List[BadRow],
    count: Int,
    token: Unique.Token,
    earliestCollectorTstamp: Option[Instant]
  )

  case class BatchedAndCompressed(
    events: Map[SchemaKey, CompressedStream],
    parseFailures: ListOfList[BadRow],
    count: Int,
    tokens: Vector[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
  )

  private def eventProcessor[F[_]: Async](
    env: Environment[F]
  ): EventProcessor[F] =
    _.through(parseBytes[F](env.cpuParallelism, env.purpose, env.badRowProcessor))
      .through(batchUpAndCompress(env.batching.maxBytes, env.batching.maxDelay, env.initialBufferSize))
      .through(writeToStorage(env.blobSink, env.appHealth, env.uploadParallelism, env.blobStorageConfig))
      .through(updateMetrics(env.metrics))
      .through(sendBadEvents(env.badSink, env.appHealth, env.badRowProcessor, env.badSinkMaxSize))
      .through(emitTokens)

  private def parseBytes[F[_]: Async](
    parallelism: Int,
    purpose: Config.Purpose,
    processor: Processor
  ): Pipe[F, TokenedEvents, ParseResult] = {
    val fold: (ParseResult, ByteBuffer) => F[ParseResult] = purpose match {
      case Config.Purpose.Enriched =>
        foldEnriched[F]
      case Config.Purpose.SDJ =>
        foldSDJ[F](processor)
    }

    in =>
      in.parEvalMap(parallelism) { case TokenedEvents(events, ack) =>
        Foldable[Chunk].foldM(events, ParseResult(Map.empty, List.empty, 0, ack, None)) { case (acc, byteBuffer) =>
          fold(acc, byteBuffer)
        }
      }
  }

  private def foldEnriched[F[_]: Sync](acc: ParseResult, byteBuffer: ByteBuffer): F[ParseResult] = Sync[F].delay {
    val str       = StandardCharsets.UTF_8.decode(byteBuffer).toString
    val timestamp = extractCollectorTstamp(str)

    ParseResult(
      Map(AtomicSchema -> List(str)) |+| acc.events, // The order is important, we want to prepend the 1-item list
      Nil,
      acc.count + 1,
      acc.token,
      chooseEarliestTstamp(acc.earliestCollectorTstamp, timestamp)
    )
  }

  private def foldSDJ[F[_]: Sync](processor: Processor)(acc: ParseResult, byteBuffer: ByteBuffer): F[ParseResult] =
    for {
      str <- Sync[F].delay(StandardCharsets.UTF_8.decode(byteBuffer.slice).toString)
      maybeSchema <- extractSchema(str, byteBuffer, processor)
      (events, parseFailures, count) = maybeSchema match {
                                         case Right(schema) =>
                                           (
                                             Map(
                                               schema -> List(str)
                                             ) |+| acc.events, // The order is important, we want to prepend the 1-item list
                                             acc.parseFailures,
                                             1
                                           )
                                         case Left(br) =>
                                           (
                                             acc.events,
                                             br :: acc.parseFailures,
                                             0
                                           )

                                       }
    } yield ParseResult(
      events,
      parseFailures,
      acc.count + count,
      acc.token,
      None
    )

  /** Extract the collector timestamp of an enriched event within TSV format. Errors are ignored. */
  private def extractCollectorTstamp(enriched: String): Option[Instant] = {
    val CollectorTstampIdx = 3
    val array              = enriched.split("\t", -1)
    if (array.length < 4)
      None
    else {
      val tz     = array(CollectorTstampIdx)
      val tstamp = tz.replaceAll(" ", "T") + "Z"
      Either.catchNonFatal(Instant.parse(tstamp)).toOption
    }
  }

  private def chooseEarliestTstamp(o1: Option[Instant], o2: Option[Instant]): Option[Instant] =
    (o1, o2)
      .mapN { case (t1, t2) =>
        if (t1.isBefore(t2)) t1 else t2
      }
      .orElse(o1)
      .orElse(o2)

  private def extractSchema[F[_]: Sync](
    str: String,
    byteBuffer: ByteBuffer,
    processor: Processor
  ): F[Either[BadRow.GenericError, SchemaKey]] =
    (for {
      parsed <- parse(str).leftMap(failure => s"Can't parse JSON holding self-describing event: ${failure.message}")
      schema <- SchemaKey.extract(parsed).leftMap(parseError => s"Can't extract schema from self-describing event: ${parseError.code}")
    } yield schema)
      .bitraverse(
        error => createBadRow(processor, error, byteBuffer),
        Sync[F].pure(_)
      )

  private def createBadRow[F[_]: Sync](
    processor: Processor,
    error: String,
    byteBuffer: ByteBuffer
  ): F[BadRow.GenericError] = Clock[F].realTimeInstant.map { now =>
    val failure = Failure.GenericFailure(
      now,
      NonEmptyList.one(error)
    )
    val payload = Payload.RawPayload(StandardCharsets.UTF_8.decode(Base64.getEncoder.encode(byteBuffer)).toString)
    BadRow.GenericError(
      processor,
      failure,
      payload
    )
  }

  private def batchUpAndCompress[F[_]: Async](
    maxBytes: Long,
    maxDelay: FiniteDuration,
    initialBufferSize: Int
  ): Pipe[F, ParseResult, BatchedAndCompressed] = {
    def go(timedPull: Pull.Timed[F, ParseResult], maybePending: Option[BatchedAndCompressed]): Pull[F, BatchedAndCompressed, Unit] =
      timedPull.uncons.flatMap {
        // End of the stream
        case None =>
          emitPending(maybePending)
        // Timeout
        case Some((Left(_), next)) =>
          emitPending(maybePending) >> go(next, None)
        // New records
        case Some((Right(chunk), next)) =>
          handleChunk(maxBytes, maxDelay, next, chunk, maybePending, initialBufferSize)
            .flatMap(go(next, _))
      }

    _.pull.timed(go(_, None)).stream
  }

  private def emitPending[F[_]](maybePending: Option[BatchedAndCompressed]): Pull[F, BatchedAndCompressed, Unit] =
    maybePending.fold(Pull.done.covaryOutput[BatchedAndCompressed])(Pull.output1)

  private def handleChunk[F[_]: Sync](
    maxBytes: Long,
    maxDelay: FiniteDuration,
    timedPull: Pull.Timed[F, _],
    chunk: Chunk[ParseResult],
    maybePending: Option[BatchedAndCompressed],
    initialBufferSize: Int
  ): Pull[F, BatchedAndCompressed, Option[BatchedAndCompressed]] = {
    val emptyBatch = BatchedAndCompressed(Map.empty, ListOfList.empty, 0, Vector.empty, None)

    def fold(
      maybePending: Option[BatchedAndCompressed],
      parseResult: ParseResult
    ): Pull[F, BatchedAndCompressed, Option[BatchedAndCompressed]] =
      for {
        pending <- maybePending.fold(timedPull.timeout(maxDelay) >> Pull.pure(emptyBatch))(Pull.pure(_))
        newPending <- Pull.eval(addParseResultToPending(pending, parseResult, initialBufferSize))
        fold <-
          if (newPending.events.view.values.map(_.size).sum > maxBytes)
            emitPending(Some(newPending)) >> timedPull.timeout(Duration.Zero) >> Pull.pure(None)
          else
            Pull.pure(Some(newPending))
      } yield fold

    Foldable[Chunk].foldM(chunk, maybePending)(fold)
  }

  private def addParseResultToPending[F[_]: Sync](
    pending: BatchedAndCompressed,
    parseResult: ParseResult,
    initialBufferSize: Int
  ): F[BatchedAndCompressed] =
    Sync[F]
      .delay {
        parseResult.events.foldLeft(pending.events) { case (acc, (schema, lines)) =>
          val compressedStream = acc.getOrElse(schema, new CompressedStream(initialBufferSize))
          compressedStream.write(lines)
          acc.updated(schema, compressedStream)
        }
      }
      .map { events =>
        BatchedAndCompressed(
          events,
          pending.parseFailures.prepend(parseResult.parseFailures),
          pending.count + parseResult.count,
          pending.tokens :+ parseResult.token,
          chooseEarliestTstamp(pending.earliestCollectorTstamp, parseResult.earliestCollectorTstamp)
        )
      }

  private def writeToStorage[F[_]: Async](
    blobSink: BlobSink[F],
    appHealth: AppHealth.Interface[F, String, RuntimeService],
    parallelism: Int,
    config: Config.BlobSink
  ): Pipe[F, BatchedAndCompressed, BatchedAndCompressed] =
    _.parEvalMap(parallelism) { batch =>
      batch.events.toList
        .parTraverse_ { case (schemaKey, events) =>
          for {
            now <- Clock[F].realTimeInstant
            path = DynamicPath.getFullPath(config.path, config.filenamePrefix, config.partitionFormat, now, schemaKey)
            _ <- blobSink.write(path, events)
          } yield ()
        }
        .onError { _ =>
          appHealth.beUnhealthyForRuntimeService(RuntimeService.BlobSink)
        }
        .as(batch)
    }

  private def updateMetrics[F[_]: Sync](metrics: Metrics[F]): Pipe[F, BatchedAndCompressed, BatchedAndCompressed] =
    _.evalTap { batch =>
      val updateCount = metrics.addCount(batch.count)
      val updateE2ELatency = batch.earliestCollectorTstamp.fold(Sync[F].unit) { timestamp =>
        for {
          now <- Sync[F].realTime
          e2eLatency = now - timestamp.toEpochMilli.millis
          _ <- metrics.setE2ELatency(e2eLatency)
        } yield ()
      }
      updateCount >> updateE2ELatency
    }

  private def sendBadEvents[F[_]: Sync](
    badSink: Sink[F],
    appHealth: AppHealth.Interface[F, String, RuntimeService],
    processor: Processor,
    maxSize: Int
  ): Pipe[F, BatchedAndCompressed, BatchedAndCompressed] =
    _.evalTap { batch =>
      if (batch.parseFailures.nonEmpty) {
        val serialized =
          batch.parseFailures.mapUnordered(badRow => BadRowsSerializer.withMaxSize(badRow, processor, maxSize))
        badSink
          .sinkSimple(serialized)
          .onError { _ =>
            appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)
          }
      } else Applicative[F].unit
    }

  private def emitTokens[F[_]]: Pipe[F, BatchedAndCompressed, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }
}
