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

import io.circe.Json

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.iglu.core.{SelfDescribingData, SchemaKey}
import com.snowplowanalytics.snowplow.scalatracker.emitters.id.AsyncEmitter

// This project
import model._
import cats.data.NonEmptyList
import cats.effect.Timer
import cats.effect.IO
import cats.Id
import scala.concurrent.ExecutionContext
import com.snowplowanalytics.snowplow.scalatracker.Emitter
import com.snowplowanalytics.snowplow.scalatracker.UUIDProvider
import java.{util => ju}

/**
 * Functionality for sending Snowplow events for monitoring purposes
 */
object SnowplowTracking {

  private val HeartbeatInterval = 300000L
  private val StorageType = "AMAZON_S3"

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  implicit val up: UUIDProvider[IO] = new UUIDProvider[IO]{ def generateUUID: IO[ju.UUID] = IO(ju.UUID.randomUUID) }
  

  /**
   * Configure a Tracker based on the configuration HOCON
   *
   * @param config The "monitoring" section of the HOCON
   * @return a new tracker instance
   */
  def initializeTracker(config: SnowplowMonitoringConfig): Tracker[IO] = {
    val endpoint = config.collectorUri
    val port = config.collectorPort
    val appName = config.appId
    val emitter: Emitter[Id] = AsyncEmitter.createAndStart(endpoint, Some(port), callback = None)
    val em: Emitter[IO] = new Emitter[IO]{
      def send(event: Emitter.EmitterPayload): IO[Unit] = IO(emitter.send(event))
    }

    Tracker(NonEmptyList.of(em), generated.Settings.name, appName)
  }

  /**
   * Send an initialization event and schedule heartbeat and shutdown events
   *
   * @param tracker a Tracker instance
   */
  def initializeSnowplowTracking(tracker: Tracker[cats.effect.IO]): Unit = {
    trackApplicationInitialization(tracker)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        trackApplicationShutdown(tracker)
      }
    })

    val heartbeatThread = new Thread {
      override def run(): Unit = {
        while (true) {
          trackApplicationHeartbeat(tracker, HeartbeatInterval)
          Thread.sleep(HeartbeatInterval)
        }
      }
    }

    heartbeatThread.start()
  }

  private def sdd(key: String, data: Json) =
    SelfDescribingData[Json](
      SchemaKey
        .fromUri(key)
        .getOrElse(
          throw new RuntimeException(
            s"Invalid SchemaKey $key. Use com.snowplowanalytics.iglu.core.SchemaKey"
          )
        ),
      data
    )

  /**
   * If a tracker has been configured, send a storage_write_failed event
   *
   * @param tracker a Tracker instance
   * @param lastRetryPeriod The backoff period after a failure
   * @param failureCount the number of consecutive failed writes
   * @param initialFailureTime Time of the first consecutive failed write
   * @param message What went wrong
   */
  def sendFailureEvent(
    tracker: Tracker[cats.effect.IO],
    lastRetryPeriod: Long,
    failureCount: Long,
    initialFailureTime: Long,
    message: String): Unit = {

    val key =
      "iglu:com.snowplowanalytics.monitoring.kinesis/storage_write_failed/jsonschema/1-0-0"

    val data = 
    Json.obj(
      ("lastRetryPeriod", Json.fromLong(lastRetryPeriod)),
      ("storage", Json.fromString(StorageType)),
      ("failureCount", Json.fromLong(failureCount)),
      ("initialFailureTime", Json.fromLong(initialFailureTime)),
      ("message" -> Json.fromString(message))
    )

    tracker.trackSelfDescribingEvent(sdd(key, data)).unsafeRunSync()
  }

  /**
   * Send an application_initialized unstructured event
   *
   * @param tracker a Tracker instance
   */
  private def trackApplicationInitialization(tracker: Tracker[cats.effect.IO]): Unit = 
    tracker.trackSelfDescribingEvent(
      sdd(
        "iglu:com.snowplowanalytics.monitoring.kinesis/app_initialized/jsonschema/1-0-0",
        Json.obj()
      )).unsafeRunSync()

  /**
   * Send an application_shutdown unstructured event
   *
   * @param tracker a Tracker instance
   */
  def trackApplicationShutdown(tracker: Tracker[cats.effect.IO]): Unit = 
    tracker.trackSelfDescribingEvent(
      sdd(
        "iglu:com.snowplowanalytics.monitoring.kinesis/app_shutdown/jsonschema/1-0-0",
        Json.obj()
      )).unsafeRunSync()

  /**
   * Send a heartbeat unstructured event
   *
   * @param tracker a Tracker instance
   * @param heartbeatInterval Time between heartbeats in milliseconds
   */
  private def trackApplicationHeartbeat(tracker: Tracker[cats.effect.IO], heartbeatInterval: Long): Unit = 
    tracker.trackSelfDescribingEvent(
      sdd(
        "iglu:com.snowplowanalytics.monitoring.kinesis/app_heartbeat/jsonschema/1-0-0",
        Json.obj(("interval", Json.fromLong(heartbeatInterval)))
      )).unsafeRunSync()

}
