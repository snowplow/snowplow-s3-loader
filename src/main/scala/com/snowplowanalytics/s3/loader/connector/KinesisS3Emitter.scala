/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.s3.loader.connector

import java.io.ByteArrayInputStream
import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

import org.slf4j.LoggerFactory

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

import cats.implicits._

import io.circe.syntax._

import com.snowplowanalytics.snowplow.badrows.BadRow.GenericError

import com.snowplowanalytics.s3.loader.{DynamicPath, Result, KinesisSink}
import com.snowplowanalytics.s3.loader.monitoring.{Monitoring, SnowplowTracking}
import com.snowplowanalytics.s3.loader.Config.{Output, Purpose, S3Output}
import com.snowplowanalytics.s3.loader.serializers.ISerializer
import com.snowplowanalytics.s3.loader.connector.KinesisS3Emitter._
import com.snowplowanalytics.s3.loader.processing.{Common, RowType}

/**
 * Emitter for flushing Kinesis event data to S3.
 *
 * Once the buffer is full, the emit function is called.
 */
class KinesisS3Emitter(client: AmazonS3,
                       monitoring: Monitoring,
                       purpose: Purpose,
                       output: Output,
                       badSink: KinesisSink,
                       serializer: ISerializer,
                      ) extends IEmitter[Result] {

  /**
   * Reads items from a buffer and saves them to s3.
   *
   * This method is expected to return a List of items that
   * failed to be written out to S3, which will be sent to
   * a Kinesis stream for bad events.
   *
   * @param buffer BasicMemoryBuffer containing EmitterInputs
   * @return list of inputs which failed transformation
   */
  def emit(buffer: UnmodifiableBuffer[Result]): java.util.List[Result] = {
    logger.info(s"Flushing buffer with ${buffer.getRecords.size} records.")

    val records = buffer.getRecords.asScala.toList
    val partitionedBatch = Common.partition(purpose, monitoring.isStatsDEnabled, records)

    val getBase: Option[String] => String =
      getBaseFilename(output.s3, buffer.getFirstSequenceNumber, buffer.getLastSequenceNumber)
    val afterEmit: () => Unit =
      () => monitoring.report(partitionedBatch.meta)

    partitionedBatch.data.flatMap {
      case (RowType.Unpartitioned, partitionRecords) if partitionRecords.nonEmpty =>
        emitRecords(partitionRecords, output.s3.path, afterEmit, getBase(None)).map(_.asLeft)
      case (data: RowType.SelfDescribing, partitionRecords) if partitionRecords.nonEmpty =>
        emitRecords(partitionRecords, output.s3.path, afterEmit, getBase(Some(data.partition))).map(_.asLeft)
      case _ =>
        records // ReadingError or empty partition - should be handled later by serializer
    }.asJava
  }

  /**
   * Closes the client when the KinesisConnectorRecordProcessor is shut down
   */
  def shutdown(): Unit =
    client.shutdown()

  /**
   * Sends records which fail deserialization or compression
   * to Kinesis with an error message
   *
   * @param records List of failed records to send to Kinesis
   */
  def fail(records: java.util.List[Result]): Unit =
    for (Left(record) <- records.asScala) {
      logger.warn(s"Record failed: ${record.payload.event}")
      logger.info("Sending failed record to Kinesis")
      badSink.store(record.asJson.noSpaces, None)
    }


  /**
   * Keep attempting to send the data to S3 until it succeeds
   *
   * @param bucket where data will be written
   * @param stream stream of rows with filename
   * @param now connection attempt start time
   * @param callback a procedure to execute after successful emit, e.g. reporting
   * @return success status of sending to S3
   */
  def attemptEmit(bucket: String, stream: ISerializer.NamedStream, now: Long, callback: () => Unit): Unit = {
    def logAndSleep(attempt: Int, e: Throwable): Unit = {
      logger.error(s"An exception during putting ${stream.filename} object to S3, attempt $attempt", e)
      monitoring.viaSnowplow { t =>
        SnowplowTracking.sendFailureEvent(t, BackoffPeriod, attempt, now, e.toString)
      }
      sleep(BackoffPeriod)
    }

    @tailrec
    def go(attempt: Int): Unit =
      if (attempt > 1 && System.currentTimeMillis() - now > output.s3.maxTimeout)
        forceShutdown()
      else {
        val request = getRequest(bucket, stream, now)
        Either.catchNonFatal(client.putObject(request)) match {
          case Right(_) => callback()
          case Left(error) =>
            logAndSleep(attempt, error)
            go(attempt + 1)
        }
      }

    go(1)
  }

  /**
   * Attempt to serialize record into a gz/lzo file and submit them to S3 via emitter
   * @param records buffered raw records
   * @param bucket where data will be written
   * @param baseFilename final filename
   * @return list of records that could not be emitted
   */
  def emitRecords(records: List[Result], bucket: String, callback: () => Unit, baseFilename: String): List[GenericError] = {
    val serializationResults = serializer.serialize(records, baseFilename)
    val (failures, successes) = serializationResults.results.separate
    val successSize = successes.size

    logger.debug(s"Successfully serialized $successSize records out of ${successSize + failures.size}")

    if (successSize > 0)
      serializationResults.namedStreams.foreach { stream =>
        attemptEmit(output.s3.bucketName, stream, System.currentTimeMillis(), callback)
      }

    failures
  }

  /**
   * Terminate the application
   *
   * Prevents shutdown hooks from running
   */
  private def forceShutdown(): Unit = {
    logger.error(s"Shutting down application as unable to connect to S3")
    monitoring.viaSnowplow { t =>
      SnowplowTracking.trackApplicationShutdown(t)
      sleep(5000)
    }
    Runtime.getRuntime.halt(1)
  }
}

object KinesisS3Emitter {

  /**
   * The amount of time to wait in between unsuccessful index requests (in milliseconds).
   * 10 seconds = 10 * 1000 = 10000
   */
  private val BackoffPeriod = 10000L

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Build a final S3 PutObject request with S3 full path, content and metadata
   * @param bucket S3 bucket name
   * @param stream name information and content
   * @param now initial connection attempt start time
   */
  def getRequest(bucket: String, stream: ISerializer.NamedStream, now: Long): PutObjectRequest = {
    val outputStream = stream.stream
    val key = DynamicPath.decorateDirectoryWithTime(stream.filename, Instant.ofEpochMilli(now))
    val inputStream = new ByteArrayInputStream(outputStream.toByteArray)
    val objMeta = new ObjectMetadata()
    objMeta.setContentLength(outputStream.size.toLong)

    new PutObjectRequest(bucket, key, inputStream, objMeta)
  }


  /**
   * Determines the filename in S3, which is the corresponding
   * Kinesis sequence range of records in the file.
   */
  def getBaseFilename(
    s3Config: S3Output,
    firstSeq: String,
    lastSeq: String
  )(
    partition: Option[String]
  ): String = {
    val path = List(s3Config.outputDirectory, s3Config.dateFormat).flatten.mkString("/")
    val fileName = (s3Config.filenamePrefix.toList ++ partition.toList ++ List(
      LocalDateTime.now.format(
        DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss")
      ),
      firstSeq,
      lastSeq
    )).mkString("-")
    val fullPath = List(path, fileName).filterNot(_.isEmpty).mkString("/")

    DynamicPath.normalize(fullPath)
  }

  /**
   * Period between retrying sending events to S3
   *
   * @param sleepTime Length of time between tries
   */
  private def sleep(sleepTime: Long): Unit =
    try Thread.sleep(sleepTime)
    catch {
      case _: InterruptedException => ()
    }
}
