/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.blob.core

import java.net.URI
import java.nio.file.Paths
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.UUID

import cats.implicits._

import com.snowplowanalytics.iglu.core.SchemaKey

object DynamicPath {
  private val schemaParts: Map[String, SchemaKey => String] = Map(
    "vendor" -> ((s: SchemaKey) => s.vendor),
    "schema" -> ((s: SchemaKey) => s.name),
    "name" -> ((s: SchemaKey) => s.name),
    "format" -> ((s: SchemaKey) => s.format),
    "model" -> ((s: SchemaKey) => s.version.model.toString)
  )

  private val timeParts = List("yyyy", "MM", "dd", "HH", "mm", "ss")

  /**
   * Validate configured partition format For enriched events it shouldn't contain some schema
   * substitution. For SDJs if it is empty then it should default to {vendor}.{schema}
   */
  def validatePartitionFormat[A, B, C](config: Config[A, B, C]): Either[String, Config[A, B, C]] =
    (config.purpose, config.output.good.partitionFormat) match {
      case (Config.Purpose.Enriched, Some(format)) =>
        Either.cond(
          !schemaParts.keySet.map("{" + _ + "}").exists(format.contains),
          config,
          "Enriched events can only get partitioned by date and time"
        )
      case (Config.Purpose.SDJ, None) =>
        // Set default partition format for SDJs
        val withDefault = config.output.good.copy(partitionFormat = Some("{vendor}.{schema}"))
        config.copy(output = config.output.copy(good = withDefault)).asRight
      case _ => config.asRight
    }

  /** Determine the path + filename based on prefix, partition format, time and event type */
  def getFullPath(
    configPath: URI,
    prefix: Option[String],
    partitionFormat: Option[String],
    now: Instant,
    schema: SchemaKey
  ): URI = {
    val fullPath = Paths
      .get(
        configPath.getHost,
        configPath.getPath,
        getPartition(partitionFormat, now, schema),
        getFilename(prefix, now)
      )
      .normalize
      .toString

    URI.create(s"${configPath.getScheme}://$fullPath")
  }

  /** Determine the subpath inside the destination, in case partitioning is enabled */
  def getPartition(
    partitionFormat: Option[String],
    now: Instant,
    schema: SchemaKey
  ): String = {
    def resolveDateTime(dateTime: Instant, template: String): String =
      timeParts.foldLeft(template) { case (acc, timePattern) =>
        acc.replaceAll(s"\\{$timePattern\\}", DateTimeFormatter.ofPattern(timePattern).withZone(ZoneId.of("UTC")).format(dateTime))
      }

    def resolveSchema(schema: SchemaKey, template: String): String = schema match {
      case AtomicSchema =>
        template
      case other =>
        schemaParts.foldLeft(template) { case (acc, (schemaPattern, getter)) =>
          acc.replaceAll(s"\\{$schemaPattern\\}", getter(other))
        }
    }

    partitionFormat match {
      case Some(template) =>
        val withDateTime = resolveDateTime(now, template)
        resolveSchema(schema, withDateTime)
      case None =>
        ""
    }
  }

  private def getFilename(prefix: Option[String], now: Instant): String = {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss").withZone(ZoneId.of("UTC"))
    val dateTime                     = formatter.format(now)
    val uuid                         = UUID.randomUUID()
    s"${prefix.getOrElse("")}$dateTime-$uuid.gz"
  }
}
