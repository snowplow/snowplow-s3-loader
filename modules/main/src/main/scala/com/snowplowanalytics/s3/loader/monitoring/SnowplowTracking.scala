/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.s3.loader.monitoring

// Scala
import com.snowplowanalytics.s3.loader.generated

import scala.concurrent.ExecutionContext.Implicits.global

// circe
import io.circe.Json

// cats
import cats.Id
import cats.data.NonEmptyList

// cats-effect
import cats.effect.Clock

// Java
import java.util.UUID
import java.util.concurrent.TimeUnit

// Iglu
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.{Tracker, UUIDProvider}
import com.snowplowanalytics.snowplow.scalatracker.emitters.id.AsyncEmitter

// This project
import com.snowplowanalytics.s3.loader.Config._

/**
 * Functionality for sending Snowplow events for monitoring purposes
 */
object SnowplowTracking {

  private val HeartbeatInterval = 300000L
  private val StorageType = "AMAZON_S3"

  /**
   * Configure a Tracker based on the configuration HOCON
   *
   * @param config The "monitoring" section of the HOCON
   * @return a new tracker instance
   */
  def initializeTracker(config: SnowplowMonitoring): Tracker[Id] = {
    implicit val clockProvider: Clock[Id] = new Clock[Id] {
      final def realTime(unit: TimeUnit): Id[Long] =
        unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
      final def monotonic(unit: TimeUnit): Id[Long] =
        unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
    }

    implicit val uuidProvider: UUIDProvider[Id] = new UUIDProvider[Id] {
      override def generateUUID: Id[UUID] = UUID.randomUUID()
    }

    val host = config.collector.getHost
    val port = if (config.collector.getPort == -1) Some(80) else None
    val https = Option(config.collector.getScheme).contains("https")
    val appName = config.appId
    val emitter = AsyncEmitter.createAndStart(host, port, https, None)
    Tracker(NonEmptyList.one(emitter), generated.Settings.name, appName)
  }

  /**
   * Send an initialization event and schedule heartbeat and shutdown events
   *
   * @param tracker a Tracker instance
   */
  def initializeSnowplowTracking(tracker: Tracker[Id]): Unit = {
    trackApplicationInitialization(tracker)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit =
        trackApplicationShutdown(tracker)
    })

    val heartbeatThread = new Thread {
      override def run(): Unit =
        while (true) {
          trackApplicationHeartbeat(tracker, HeartbeatInterval)
          Thread.sleep(HeartbeatInterval)
        }
    }

    heartbeatThread.start()
  }

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
    tracker: Tracker[Id],
    lastRetryPeriod: Long,
    failureCount: Int,
    initialFailureTime: Long,
    message: String
  ): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData[Json](
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "storage_write_failed",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.obj(
          ("lastRetryPeriod", Json.fromLong(lastRetryPeriod)),
          ("storage" -> Json.fromString(StorageType)),
          ("failureCount" -> Json.fromInt(failureCount)),
          ("initialFailureTime" -> Json.fromLong(initialFailureTime)),
          ("message" -> Json.fromString(message))
        )
      )
    )

  /**
   * Send an application_initialized unstructured event
   *
   * @param tracker a Tracker instance
   */
  private def trackApplicationInitialization(tracker: Tracker[Id]): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData[Json](
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_initialized",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.Null
      )
    )

  /**
   * Send an application_shutdown unstructured event
   *
   * @param tracker a Tracker instance
   */
  def trackApplicationShutdown(tracker: Tracker[Id]): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData[Json](
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_shutdown",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.Null
      )
    )

  /**
   * Send a heartbeat unstructured event
   *
   * @param tracker a Tracker instance
   * @param heartbeatInterval Time between heartbeats in milliseconds
   */
  private def trackApplicationHeartbeat(tracker: Tracker[Id], heartbeatInterval: Long): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData[Json](
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_heartbeat",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.obj(
          ("interval" -> Json.fromLong(heartbeatInterval))
        )
      )
    )
}
