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

//Java
import java.io.ByteArrayInputStream

// Scala
import scala.util.control.NonFatal
import scala.collection.JavaConversions._

// SLF4j
import org.slf4j.LoggerFactory

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// cats
import cats.data.Validated

// AWS libs
import com.amazonaws.AmazonServiceException
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.auth.AWSCredentialsProvider

// json4s
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// This project
import sinks._
import serializers._
import model._

/**
 * Emitter for flushing data to S3.
 *
 * @param config S3Loader configuration
 * @param provider AWSCredentialsProvider
 * @param badSink Sink instance for not sent data
 * @param maxConnectionTime Max time for attempting to send S3
 * @param tracker Tracker instance
 */
class S3Emitter(
  config: S3Config,
  provider: AWSCredentialsProvider,
  badSink: ISink,
  maxConnectionTime: Long,
  tracker: Option[Tracker]
) {

  // create Amazon S3 Client
  val log = LoggerFactory.getLogger(getClass)
  val client = AmazonS3ClientBuilder
    .standard()
    .withCredentials(provider)
    .withEndpointConfiguration(new EndpointConfiguration(config.endpoint, config.region))
    .withPathStyleAccessEnabled(config.pathStyleAccessEnabled)
    .build()

  /**
  * The amount of time to wait in between unsuccessful index requests (in milliseconds).
  * 10 seconds = 10 * 1000 = 10000
  */
  private val BackoffPeriod = 10000L
  private val TstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  /**
  * Period between retrying sending events to S3
  *
  * @param sleepTime Length of time between tries
  */
  private def sleep(sleepTime: Long): Unit = {
    try {
      Thread.sleep(sleepTime)
    } catch {
        case _: InterruptedException => ()
    }
  }

  /**
  * Terminate the application 
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
  * Returns an ISO valid timestamp
  *
  * @param tstamp The Timestamp to convert
  * @return the formatted Timestamp
  */
  private def getTimestamp(tstamp: Long): String = {
    val dt = new DateTime(tstamp)
    TstampFormat.print(dt)
  }

  /**
  * Sends records which fail deserialization, compression or partitioning
  *
  * @param records List of failed records
  */
  def sendFailures(records: java.util.List[EmitterInput]): Unit = {
    for (Validated.Invalid(record) <- records.toList) {
      log.warn(s"Record failed: ${record.line}")
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
  * Keep attempting to send the data to S3 until it succeeds
  *
  * @param namedStream stream of rows with filename
  * @param partition whether to send rows into `s3.bucketJson` (true) or `s3.bucket` (false)
  * @return success status of sending to S3
  */
  def attemptEmit(namedStream: NamedStream, partition: Boolean, connectionAttemptStartTime: Long): Boolean = {

    var attemptCount: Long = 1

    def logAndSleep(e: Throwable): Unit = {
      tracker.foreach {
        t => SnowplowTracking.sendFailureEvent(t, BackoffPeriod, connectionAttemptStartTime, attemptCount, e.toString)
      }
      attemptCount = attemptCount + 1
      sleep(BackoffPeriod)

    }
    val connectionAttemptStartDateTime = new DateTime(connectionAttemptStartTime)
    while (true) {
      if (attemptCount > 1 && System.currentTimeMillis() - connectionAttemptStartTime > maxConnectionTime) {
        forceShutdown()
      }

      try {
        val outputStream = namedStream.stream

        val s3Key = DynamicPath.decorateDirectoryWithTime(namedStream.filename, connectionAttemptStartDateTime)

        val inputStream = new ByteArrayInputStream(outputStream.toByteArray)

        val objMeta = new ObjectMetadata()
        objMeta.setContentLength(outputStream.size.toLong)
        val bucket = if (partition) config.partitionedBucket.getOrElse(config.bucket) else config.bucket
        client.putObject(bucket, s3Key, inputStream, objMeta)

        return true
      } catch {
        // Retry on failure
        case e: AmazonServiceException =>
          log.error("S3 could not process the request", e)
          logAndSleep(e)
        case NonFatal(e) =>
          log.error("S3Emitter threw an unexpected exception", e)
          logAndSleep(e)
      }
    }
    false
  }
}
