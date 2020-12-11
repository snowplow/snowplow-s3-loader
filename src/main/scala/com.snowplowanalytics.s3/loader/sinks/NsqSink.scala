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
package sinks

// NSQ
import com.snowplowanalytics.client.nsq.NSQProducer

// This project
import model._

/**
 * NSQ sink
 *
 * @param config Configuration for NSQ
 */
class NsqSink(config: S3LoaderConfig) extends ISink {

  private val producer = new NSQProducer().addAddress(config.nsq.host, config.nsq.port).start()

  /**
   * Write a record to the NSQ stream
   *
   * @param output The string record to write
   * @param key Unused parameter which exists to extend ISink
   * @param good Unused parameter which exists to extend ISink
   */
  override def store(
    output: String,
    key: Option[String],
    good: Boolean
  ): Unit =
    producer.produce(config.streams.outStreamName, output.getBytes())
}
