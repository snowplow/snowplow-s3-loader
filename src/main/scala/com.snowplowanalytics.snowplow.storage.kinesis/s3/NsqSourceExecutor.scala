/**
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd.
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

package com.snowplowanalytics.snowplow.storage.kinesis.s3

// NSQ
import com.snowplowanalytics.client.nsq.NSQConsumer
import com.snowplowanalytics.client.nsq.lookup.DefaultNSQLookup
import com.snowplowanalytics.client.nsq.NSQMessage
import com.snowplowanalytics.client.nsq.NSQConfig
import com.snowplowanalytics.client.nsq.callbacks.NSQMessageCallback
import com.snowplowanalytics.client.nsq.callbacks.NSQErrorCallback
import com.snowplowanalytics.client.nsq.exceptions.NSQException

// Scala
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// Scalaz
import scalaz._
import Scalaz._

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// Logging
import org.slf4j.LoggerFactory

// This project
import sinks._
import serializers._

/**
 * Executor for NSQSource
 *
 * @param config the kinesis config for getting informations for S3
 * @param nsqConfig the NSQ configuration
 * @param badSink the configured BadSink
 * @param serializer the instance of one of the serializer
 * @param maxConnectionTime max time for trying to connect S3 instance
 */
class NsqSourceExecutor(
  config: KinesisConnectorConfiguration,
  nsqConfig: S3LoaderNsqConfig,
  badSink: ISink,
  serializer: ISerializer,
  maxConnectionTime: Long,
  tracker: Option[Tracker]
) extends Runnable {

  lazy val log = LoggerFactory.getLogger(getClass())

  //nsq messages will be buffered in msgBuffer until buffer size become equal to nsqBufferSize
  val msgBuffer = new ListBuffer[EmitterInput]()

  val s3Emitter = new S3Emitter(config, badSink, maxConnectionTime, tracker)
  private val TimeFormat = DateTimeFormat.forPattern("HH:mm:ss.SSS").withZone(DateTimeZone.UTC)
  private val DateFormat = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC)

  private def getBaseFilename(startTime: Long, endTime: Long): String = {
    val currentTimeObject = new DateTime(System.currentTimeMillis())
    val startTimeObject = new DateTime(startTime)
    val endTimeObject = new DateTime(endTime)

    DateFormat.print(currentTimeObject) + "-" +
      TimeFormat.print(startTimeObject) + "-" +
      TimeFormat.print(endTimeObject)   + "-" +
      math.abs(util.Random.nextInt)
  }

  override def run: Unit = {

    val nsqCallback = new  NSQMessageCallback {
      //start time of filling the buffer
      var bufferStartTime = System.currentTimeMillis()
      val nsqBufferSize = config.streams.buffer.recordLimit

      override def message(msg: NSQMessage): Unit = {
        val validMsg = msg.getMessage.success
        msgBuffer.synchronized {
          msgBuffer += validMsg
          msg.finished()
          if (msgBuffer.size >= nsqBufferSize) {
            //finish time of filling the buffer
            val bufferEndTime = System.currentTimeMillis()
            val baseFilename = getBaseFilename(bufferStartTime, bufferEndTime)
            val serializationResults = serializer.serialize(msgBuffer.toList, baseFilename)
            val (successes, failures) = serializationResults.results.partition(_.isSuccess)

            log.info(s"Successfully serialized ${successes.size} records out of ${successes.size + failures.size}")

            if (successes.size > 0) {
              serializationResults.namedStreams.foreach {
                val connectionAttemptStartTime = System.currentTimeMillis()
                s3Emitter.attemptEmit(_, connectionAttemptStartTime) match {
                  case false => log.error(s"Error while sending to S3")
                  case true => log.info(s"Successfully sent ${successes.size} records")
                }
              }
            }

            if (failures.size > 0) {
              s3Emitter.sendFailures(failures)
            }

            msgBuffer.clear()
            //make buffer start time of the next buffer the buffer finish time of the current buffer
            bufferStartTime = bufferEndTime
          }
        }
      }
    }

    val errorCallback = new NSQErrorCallback {
      override def error(e: NSQException) =
        log.error(s"Exception while consuming topic $nsqConfig.nsqGoodSourceTopicName", e)
    }

    val lookup = new DefaultNSQLookup
    // use NSQLookupd
    lookup.addLookupAddress(nsqConfig.nsqHost, nsqConfig.nsqlookupPort)
    val consumer = new NSQConsumer(lookup,
                                   nsqConfig.nsqSourceTopicName,
                                   nsqConfig.nsqSourceChannelName,
                                   nsqCallback,
                                   new NSQConfig(),
                                   errorCallback)
    consumer.start()
  }
}