/*
 * Copyright (c) 2014-2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.kinesis.s3

import scala.collection.JavaConverters._

// Java libs
import java.io.{
  OutputStream,
  DataOutputStream,
  ByteArrayInputStream,
  ByteArrayOutputStream,
  IOException
}
import java.util.Calendar
import java.text.SimpleDateFormat

// Java lzo
import org.apache.hadoop.conf.Configuration
import com.hadoop.compression.lzo.LzopCodec

// Elephant bird
import com.twitter.elephantbird.mapreduce.io.ThriftBlockWriter

// SLF4j
import org.slf4j.LoggerFactory

// AWS libs
import com.amazonaws.AmazonServiceException
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata

// AWS Kinesis connector libs
import com.amazonaws.services.kinesis.connectors.{
  UnmodifiableBuffer,
  KinesisConnectorConfiguration
}
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

// Scala
import scala.collection.JavaConversions._
import scala.util.control.NonFatal
import scala.annotation.tailrec

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// This project
import sinks._
import serializers._

/**
 * Emitter for flushing Kinesis event data to S3.
 *
 * Once the buffer is full, the emit function is called.
 */
class S3Emitter(config: KinesisConnectorConfiguration, badSink: ISink, serializer: ISerializer, maxConnectionTime: Long, tracker: Option[Tracker]) extends IEmitter[EmitterInput] {

  /**
   * The amount of time to wait in between unsuccessful index requests (in milliseconds).
   * 10 seconds = 10 * 1000 = 10000
   */
  private val BackoffPeriod = 10000L

  // An ISO valid timestamp formatter
  private val TstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  val bucket = config.S3_BUCKET
  val log = LoggerFactory.getLogger(getClass)
  val client = AmazonS3ClientBuilder
    .standard()
    .withCredentials(config.AWS_CREDENTIALS_PROVIDER)
    .withEndpointConfiguration(new EndpointConfiguration(config.S3_ENDPOINT, config.REGION_NAME))
    .build()

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  /**
   * Determines the filename in S3, which is the corresponding
   * Kinesis sequence range of records in the file.
   */
  protected def getBaseFilename(firstSeq: String, lastSeq: String): String = {
    dateFormat.format(Calendar.getInstance().getTime()) +
      "-" + firstSeq + "-" + lastSeq
  }

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
  override def emit(buffer: UnmodifiableBuffer[EmitterInput]): java.util.List[EmitterInput] = {

    log.info(s"Flushing buffer with ${buffer.getRecords.size} records.")

    val records = buffer.getRecords().asScala.toList
    val baseFilename = getBaseFilename(buffer.getFirstSequenceNumber, buffer.getLastSequenceNumber)
    val serializationResults = serializer.serialize(records, baseFilename)
    val (successes, failures) = serializationResults.results.partition(_.isSuccess)

    log.info(s"Successfully serialized ${successes.size} records out of ${successes.size + failures.size}")

    val connectionAttemptStartTime = System.currentTimeMillis()

    /**
     * Keep attempting to send the data to S3 until it succeeds
     *
     * @return list of inputs which failed to be sent to S3
     */
    def attemptEmit(namedStream: NamedStream): Boolean = {

      var attemptCount: Long = 1

      while (true) {
        if (attemptCount > 1 && System.currentTimeMillis() - connectionAttemptStartTime > maxConnectionTime) {
          forceShutdown()
        }

        try {
          val outputStream = namedStream.stream
          val filename = namedStream.filename
          val inputStream = new ByteArrayInputStream(outputStream.toByteArray)

          val objMeta = new ObjectMetadata()
          objMeta.setContentLength(outputStream.size.toLong)

          client.putObject(bucket, filename, inputStream, objMeta)

          log.info(s"Successfully emitted ${successes.size} records to S3 in s3://${bucket}/${filename}")

          // Return the failed records
          return true
        } catch {
          // Retry on failure
          case ase: AmazonServiceException => {
            log.error("S3 could not process the request", ase)
            tracker match {
              case Some(t) => SnowplowTracking.sendFailureEvent(t, BackoffPeriod, connectionAttemptStartTime, attemptCount, ase.toString)
              case None => None
            }
            attemptCount = attemptCount + 1
            sleep(BackoffPeriod)
          }
          case NonFatal(e) => {
            log.error("S3Emitter threw an unexpected exception", e)
            tracker match {
              case Some(t) => SnowplowTracking.sendFailureEvent(t, BackoffPeriod, connectionAttemptStartTime, attemptCount, e.toString)
              case None => None
            }
            attemptCount = attemptCount + 1
            sleep(BackoffPeriod)
          }
        }
      }
      false
    }

    if (successes.size > 0) {
      serializationResults.namedStreams.foreach { attemptEmit(_) }
      failures
    } else {
      failures
    }
  }

  /**
   * Terminate the application in a way the KCL cannot stop
   *
   * Prevents shutdown hooks from running
   */
  private def forceShutdown(): Unit = {
    log.error(s"Shutting down application as unable to connect to S3 for over $maxConnectionTime ms")
    tracker foreach {
      t =>
        SnowplowTracking.trackApplicationShutdown(t)
        sleep(5000)
    }
    Runtime.getRuntime.halt(1)
  }

  /**
   * Closes the client when the KinesisConnectorRecordProcessor is shut down
   */
  override def shutdown(): Unit = {
    client.shutdown
  }

  /**
   * Sends records which fail deserialization or compression
   * to Kinesis with an error message
   *
   * @param records List of failed records to send to Kinesis
   */
  override def fail(records: java.util.List[EmitterInput]): Unit = {
    // TODO: Should there be a check for Successes?
    for (Failure(record) <- records.toList) {
      log.warn(s"Record failed: $record.line")
      log.info("Sending failed record to Kinesis")
      val output = compact(render(
        ("line" -> record.line) ~ 
        ("errors" -> record.errors) ~
        ("failure_tstamp" -> getTimestamp(System.currentTimeMillis()))
      ))
      badSink.store(output, Some("key"), false)
    }
  }

  /**
   * Period between retrying sending events to S3
   *
   * @param sleepTime Length of time between tries
   */
  private def sleep(sleepTime: Long): Unit = {
    try {
      Thread.sleep(sleepTime)
    } catch {
      case e: InterruptedException => ()
    }
  }

  /**
   * Returns an ISO valid timestamp
   *
   * @param tstamp The Timestamp to convert
   * @return the formatted Timestamp
   */
  private def getTimestamp(tstamp: Long): String = {
    val dt = new DateTime(tstamp)
    TstampFormat.print(dt)
  }
}
