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

import java.time.Duration
import java.util

// Kafka
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

// Logger
import org.slf4j.Logger
import org.slf4j.LoggerFactory

// Scala
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// cats
import cats.syntax.validated._

//AWS libs
import com.amazonaws.auth.AWSCredentialsProvider

// This project
import com.snowplowanalytics.s3.loader.model._
import com.snowplowanalytics.s3.loader.serializers._
import com.snowplowanalytics.s3.loader.sinks._

/**
  * Executor for KafkaSource
  *
  * @param config            S3Loader configuration
  * @param provider          AWSCredentialsProvider
  * @param badSink           Configured BadSink
  * @param serializer        Serializer instance
  * @param maxConnectionTime Max time for trying to connect S3 instance
  */
class KafkaSourceExecutor(config: S3LoaderConfig,
                          provider: AWSCredentialsProvider,
                          badSink: ISink,
                          val serializer: ISerializer,
                          maxConnectionTime: Long,
                          tracker: Option[Tracker])
    extends Runnable
    with IBufferedOutput {
  lazy val log: Logger = LoggerFactory.getLogger(getClass)
  lazy val s3Config: S3Config = config.s3
  private val kafkaConsumer = {
    val consumer =
      new KafkaConsumer[String, RawRecord](config.kafka.properties)
    val topicName = config.streams.inStreamName
    val topics = topicName :: Nil
    var seeked = false
    consumer.subscribe(
      topics,
      new ConsumerRebalanceListener() {
        override def onPartitionsRevoked(
          partitions: util.Collection[TopicPartition]
        ): Unit = {}

        override def onPartitionsAssigned(
          partitions: util.Collection[TopicPartition]
        ): Unit = {
          if (!config.kafka.startFromBeginning || seeked) {
            return
          }
          consumer.seekToBeginning(partitions)
          seeked = true
        }
      }
    )
    consumer
  }
  private val pollTime = Duration.ofMillis(config.kafka.pollTime.getOrElse(1000))
  private val msgBuffer = new ListBuffer[EmitterInput]()
  val s3Emitter =
    new S3Emitter(config.s3, provider, badSink, maxConnectionTime, tracker)
  private var bufferStartTime = System.currentTimeMillis()

  override def run(): Unit = {
    while (true) {
      val records = kafkaConsumer.poll(pollTime)
      log.debug("Received %d records", records.count())

      records.foreach(record => {
        log.debug(
          s"Processing record ${record.key()} partition id ${record.partition()}"
        )
        val validMsg = record.value().valid
        msgBuffer += validMsg
        if (shouldFlush) flush()
      })
    }
  }

  private def shouldFlush: Boolean = {
    msgBuffer.nonEmpty && (msgBuffer.length >= config.streams.buffer.recordLimit || timerDepleted())
  }

  private def timerDepleted(): Boolean = {
    (System
      .currentTimeMillis() - bufferStartTime) > config.streams.buffer.timeLimit
  }

  private def flush(): Unit = {
    val bufferEndTime = System.currentTimeMillis()
    flushMessages(msgBuffer.toList, bufferStartTime, bufferEndTime)
    msgBuffer.clear()
    bufferStartTime = bufferEndTime
    kafkaConsumer.commitSync()
  }
}
