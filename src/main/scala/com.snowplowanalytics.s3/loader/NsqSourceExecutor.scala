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

// NSQ
import com.snowplowanalytics.client.nsq.NSQConsumer
import com.snowplowanalytics.client.nsq.lookup.DefaultNSQLookup
import com.snowplowanalytics.client.nsq.NSQMessage
import com.snowplowanalytics.client.nsq.NSQConfig
import com.snowplowanalytics.client.nsq.callbacks.NSQMessageCallback
import com.snowplowanalytics.client.nsq.callbacks.NSQErrorCallback
import com.snowplowanalytics.client.nsq.exceptions.NSQException
import org.slf4j.Logger

// Scala
import scala.collection.mutable.ListBuffer

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// cats
import cats.syntax.validated._

//AWS libs
import com.amazonaws.auth.AWSCredentialsProvider

// Logging
import org.slf4j.LoggerFactory

// This project
import sinks._
import serializers._
import model._

/**
 * Executor for NSQSource
 *
 * @param config S3Loader configuration
 * @param provider AWSCredentialsProvider
 * @param badSink Configured BadSink
 * @param serializer Serializer instance
 * @param maxConnectionTime Max time for trying to connect S3 instance
 */
class NsqSourceExecutor(
  config: S3LoaderConfig,
  provider: AWSCredentialsProvider,
  badSink: ISink,
  val serializer: ISerializer,
  maxConnectionTime: Long,
  tracker: Option[Tracker]
) extends Runnable with IBufferedOutput {

  lazy val log: Logger = LoggerFactory.getLogger(getClass)
  lazy val s3Config: S3Config = config.s3

  //nsq messages will be buffered in msgBuffer until buffer size become equal to nsqBufferSize
  val msgBuffer = new ListBuffer[EmitterInput]()

  val s3Emitter =
    new S3Emitter(config.s3, provider, badSink, maxConnectionTime, tracker)

  override def run: Unit = {

    val nsqCallback = new NSQMessageCallback {
      //start time of filling the buffer
      var bufferStartTime = System.currentTimeMillis()
      val nsqBufferSize = config.streams.buffer.recordLimit

      override def message(msg: NSQMessage): Unit = {
        val validMsg = msg.getMessage.valid
        msgBuffer.synchronized {
          msgBuffer += validMsg
          msg.finished()
          if (msgBuffer.size >= nsqBufferSize) {
            //finish time of filling the buffer
            val bufferEndTime = System.currentTimeMillis()
            flushMessages(msgBuffer.toList, bufferStartTime, bufferEndTime)

            msgBuffer.clear()
            //make buffer start time of the next buffer the buffer finish time of the current buffer
            bufferStartTime = bufferEndTime
          }
        }
      }
    }

    val errorCallback = new NSQErrorCallback {
      override def error(e: NSQException) =
        log.error(
          s"Exception while consuming topic $config.streams.inStreamName",
          e
        )
    }

    val lookup = new DefaultNSQLookup
    // use NSQLookupd
    lookup.addLookupAddress(config.nsq.host, config.nsq.lookupPort)
    val consumer = new NSQConsumer(
      lookup,
      config.streams.inStreamName,
      config.nsq.channelName,
      nsqCallback,
      new NSQConfig(),
      errorCallback
    )
    consumer.start()
  }
}
