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
package com.snowplowanalytics.s3.loader.sinks

// Kafka
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

// Logging
import org.slf4j.LoggerFactory

// Concurrent libraries
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Random, Success}
import scala.util.Failure

// This project
import com.snowplowanalytics.s3.loader.model.S3LoaderConfig

/**
 * Kafka sink
 *
 * @param config Configuration for Kafka
 */
class KafkaSink(config: S3LoaderConfig) extends ISink {
  private val log = LoggerFactory.getLogger(getClass)
  private lazy val producer = new KafkaProducer[String, String](config.kafka.properties)

  private def put(record: ProducerRecord[String, String]): Future[RecordMetadata] = Future {
    producer.send(record).get()
  }

  /**
   * Write a record to the Kafka topic
   *
   * @param output The string record to write
   * @param key    Unused parameter which exists to extend ISink
   * @param good   Unused parameter which exists to extend ISink
   */
  override def store(output: String, key: Option[String], good: Boolean): Unit = {
    val record = new ProducerRecord[String, String](config.streams.outStreamName, key.getOrElse(Random.nextString(6)), output)
    put(record) onComplete {
      case Success(result) =>
        log.info("Writing successful")
        log.info(s"  + PartitionId: ${result.partition()}")
        log.info(s"  + Offset: ${result.offset()}")
      case Failure(f) =>
        log.error("Writing failed")
        log.error("  + " + f.getMessage)
    }
  }
}

