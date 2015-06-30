 /*
 * Copyright (c) 2014-2015 Snowplow Analytics Ltd.
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

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Config
import com.typesafe.config.Config

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.snowplow.scalatracker.SelfDescribingJson
import com.snowplowanalytics.snowplow.scalatracker.emitters.AsyncEmitter

/**
 * Functionality for sending Snowplow events for monitoring purposes
 */
object SnowplowTracking {

  private val HeartbeatInterval = 300000L

  /**
   * Configure a Tracker based on the configuration HOCON
   *
   * @param config The "monitoring.snowplow" section of the HOCON
   * @return a new tracker instance
   */
  def initializeTracker(config: Config): Tracker = {
    val endpoint = config.getString("collector-uri")
    val port = config.getInt("collector-port")
    val appName = config.getString("app-id")
    // Not yet used
    val method = config.getString("method")
    val emitter = AsyncEmitter.createAndStart(endpoint, port)
    new Tracker(List(emitter), generated.Settings.name, appName)
  }

  /**
   * Send an initialization event and schedule heartbeat and shutdown events
   *
   * @param tracker a Tracker instance
   */
  def initializeSnowplowTracking(tracker: Tracker) {
    trackApplicationInitialization(tracker)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        trackApplicationShutdown(tracker)
      }
    })

    val heartbeatThread = new Thread {
      override def run() {
        while (true) {
          trackApplicationHeartbeat(tracker, HeartbeatInterval)
          Thread.sleep(HeartbeatInterval)
        }
      }
    }

    heartbeatThread.start()
  }

  /**
   * If a tracker has been configured, send a sink_write_failed event
   *
   * @param tracker a Tracker instance
   * @param lastRetryPeriod The backoff period after a failure
   * @param message What went wrong
   * @param sinkType The type of sink in which the failure occured
   */
  def sendFailureEvent(
    tracker: Tracker,
    lastRetryPeriod: Long,
    message: String,
    sinkType: String) {

    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.snowplow/sink_write_failed/jsonschema/1-0-0",
      ("lastRetryPeriod" -> lastRetryPeriod) ~
      ("sink" -> sinkType) ~
      ("message" -> message)
    ))
  }

  /**
   * Send an application_initialized unstructured event
   *
   * @param tracker a Tracker instance
   */
  private def trackApplicationInitialization(tracker: Tracker) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.snowplow/application_initialized/jsonschema/1-0-0",
      JObject(Nil)
    ))
  }

  /**
   * Send an application_shutdown unstructured event
   *
   * @param tracker a Tracker instance
   */
  def trackApplicationShutdown(tracker: Tracker) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.snowplow/application_shutdown/jsonschema/1-0-0",
      JObject(Nil)
    ))
  }

  /**
   * Send a heartbeat unstructured event
   *
   * @param tracker a Tracker instance
   * @param heartbeatInterval Time between heartbeats in milliseconds
   */
  private def trackApplicationHeartbeat(tracker: Tracker, heartbeatInterval: Long) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.snowplow/heartbeat/jsonschema/1-0-0",
      "interval" -> heartbeatInterval
    ))
  }
}
