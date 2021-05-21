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

package com.snowplowanalytics.s3.loader

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.{Random, Failure, Success}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import com.amazonaws.services.kinesis.model._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.metrics.RequestMetricCollector
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}

import org.slf4j.LoggerFactory

/**
 * Configuration for the Kinesis stream
 * Used only as a sink for bad rows (events that couldn't be sunk into S3)
 *
 * @param client Amazon Kinesis SDK client
 * @param name Kinesis stream name
 */
class KinesisSink(client: AmazonKinesis, name: String) {

  private val log = LoggerFactory.getLogger(getClass)

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
        case _: ResourceNotFoundException => false
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
   */
  def store(output: String, key: Option[String]): Unit =
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

object KinesisSink {
  def build(config: Config): KinesisSink = {
    val client = AmazonKinesisClientBuilder.standard()
    config.input.customEndpoint match {
      case Some(value) => client.setEndpointConfiguration(new EndpointConfiguration(value, config.region.orNull))
      case None => ()
    }

    config.monitoring.flatMap(_.metrics.flatMap(_.cloudWatch)) match {
      case Some(enable) if enable => ()
      case None => client.setMetricsCollector(RequestMetricCollector.NONE)
    }

    new KinesisSink(client.build(), config.output.badStreamName)
  }
}