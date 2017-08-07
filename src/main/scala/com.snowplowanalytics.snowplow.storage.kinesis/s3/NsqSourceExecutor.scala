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

//NSQ
import com.github.brainlag.nsq.NSQConsumer
import com.github.brainlag.nsq.lookup.DefaultNSQLookup
import com.github.brainlag.nsq.NSQMessage
import com.github.brainlag.nsq.callbacks.NSQMessageCallback

//Java
import java.nio.charset.StandardCharsets

// Scala
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// Scalaz
import scalaz._
import Scalaz._

// AWS Kinesis connector libs
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration
}

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// This project
import sinks._
import serializers._


/**
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

  //initialize S3EmitterUtils class
  val s3EmitterUtils = new S3EmitterUtils(config, badSink, maxConnectionTime, tracker)

  private val TimeFormat = DateTimeFormat.forPattern("HH:mm:ss").withZone(DateTimeZone.UTC)
  private val DateFormat = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC)

  private def getBaseFilename(startTime: Long, endTime: Long): String = {
    val currentTimeObject = new DateTime(System.currentTimeMillis())
    val startTimeObject = new DateTime(startTime)
    val endTimeObject = new DateTime(endTime)

    DateFormat.print(currentTimeObject) + "-" + 
    TimeFormat.print(startTimeObject)   + "-" + 
    TimeFormat.print(endTimeObject)
  }
  
  private def convertRawRecord(rawRecord: RawRecord): EmitterInput = rawRecord.success
    
  override def run: Unit = {

    val nsqCallback = new  NSQMessageCallback {
      //nsq messages will be buffered in msgBuffer until buffer size become equal to nsqBufferSize
      val msgBuffer = new ListBuffer[EmitterInput]()
      //start time of filling the buffer
      var bufferStartTime = System.currentTimeMillis()

      def message(msg: NSQMessage): Unit = {

        val nsqBufferSize = nsqConfig.nsqBufferSize
        val validMsg = convertRawRecord(msg.getMessage)
        val connectionAttemptStartTime = System.currentTimeMillis()

        msgBuffer.synchronized {
          msgBuffer += validMsg
            if (msgBuffer.size == nsqBufferSize) {
              //finish time of filling the buffer
              val bufferEndTime = System.currentTimeMillis()
              val baseFilename = getBaseFilename(bufferStartTime, bufferEndTime)
              val serializationResults = serializer.serialize(msgBuffer.toList, baseFilename)
              val (successes, failures) = serializationResults.results.partition(_.isSuccess)

              if (successes.size > 0) {
                serializationResults.namedStreams.foreach { s3EmitterUtils.attemptEmit(_, connectionAttemptStartTime) }
              }

              if (failures.size > 0) {
                s3EmitterUtils.sendFailures(failures)
              }

              msgBuffer.clear()
              //make buffer start time of the next buffer the buffer finish time of the current buffer 
              bufferStartTime = bufferEndTime
            }                
        }
        msg.finished() 
      }
    }

    val lookup = new DefaultNSQLookup
    // use NSQLookupd
    lookup.addLookupAddress(nsqConfig.nsqHost, nsqConfig.nsqlookupdPort)
    val consumer = new NSQConsumer(lookup, nsqConfig.nsqGoodSourceTopicName, nsqConfig.nsqGoodSourceChannelName, nsqCallback)
    consumer.start() 
  }   
}