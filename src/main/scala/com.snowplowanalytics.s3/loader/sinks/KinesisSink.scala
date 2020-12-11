/*
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

package com.snowplowanalytics.s3.loader.sinks

// Java
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

// Scala
import scala.util.Random

// Amazon
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model._
import com.amazonaws.metrics.RequestMetricCollector

// Concurrent libraries
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

// Logging
import org.slf4j.LoggerFactory

// cats
import cats.Id

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

/**
 * Configuration for the Kinesis stream
 *
 * @param provider AWSCredentialsProvider
 * @param endpoint Kinesis stream endpoint
 * @param region Kinesis stream region
 * @param name Kinesis stream name
 * @param tracker Snowplow Tracker instance
 */
class KinesisSink(
  provider: AWSCredentialsProvider,
  endpoint: String,
  region: String,
  name: String,
  tracker: Option[Tracker[Id]],
  disableCloudWatch: Boolean
) extends ISink {

  private val log = LoggerFactory.getLogger(getClass)

  // Explicitly create a client so we can configure the end point
  val client =
    if (disableCloudWatch)
      AmazonKinesisClientBuilder
        .standard()
        .withCredentials(provider)
        .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
        .withMetricsCollector(RequestMetricCollector.NONE)
        .build()
    else
      AmazonKinesisClientBuilder
        .standard()
        .withCredentials(provider)
        .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
        .build()

  require(streamExists(name), s"Kinesis stream $name doesn't exist")

  /**
   * Checks if a stream exists.
   *
   * @param name Name of the stream to look for
   * @return Whether the stream both exists and is active
   */
  private def streamExists(name: String): Boolean = {
    val exists =
      try {
        val describeStreamResult = client.describeStream(name)
        describeStreamResult.getStreamDescription.getStreamStatus == "ACTIVE"
      } catch {
        case rnfe: ResourceNotFoundException => false
      }

    if (exists)
      log.info(s"Stream $name exists and is active")
    else
      log.info(s"Stream $name doesn't exist or is not active")

    exists
  }

  private def put(
    name: String,
    data: ByteBuffer,
    key: String
  ): Future[PutRecordResult] =
    Future {
      val putRecordRequest = {
        val p = new PutRecordRequest()
        p.setStreamName(name)
        p.setData(data)
        p.setPartitionKey(key)
        p
      }
      client.putRecord(putRecordRequest)
    }

  /**
   * Write a record to the Kinesis stream
   *
   * @param output The string record to write
   * @param key A hash of the key determines to which shard the
   *            record is assigned. Defaults to a random string.
   * @param good Unused parameter which exists to extend ISink
   */
  def store(
    output: String,
    key: Option[String],
    good: Boolean
  ): Unit =
    put(name, ByteBuffer.wrap(output.getBytes(UTF_8)), key.getOrElse(Random.nextInt.toString)) onComplete {
      case Success(result) =>
        log.info("Writing successful")
        log.info(s"  + ShardId: ${result.getShardId}")
        log.info(s"  + SequenceNumber: ${result.getSequenceNumber}")
      case Failure(f) =>
        log.error("Writing failed")
        log.error("  + " + f.getMessage)
    }
}
