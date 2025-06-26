/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.s3.loader.serializers

// Java libs
import java.io.{ByteArrayOutputStream, IOException}
import java.time.Instant

// Scala
import scala.util.control.NonFatal

// cats
import cats.syntax.either._
import cats.data.NonEmptyList

// SLF4j
import org.slf4j.LoggerFactory

// Apache commons
import org.apache.commons.codec.binary.Base64

import com.snowplowanalytics.snowplow.badrows.BadRow.GenericError
import com.snowplowanalytics.snowplow.badrows.Failure.GenericFailure
import com.snowplowanalytics.snowplow.badrows.Payload.RawPayload

import com.snowplowanalytics.s3.loader.{RawRecord, Result, S3Loader}

/**
 * Shared interface for all serializers
 */
trait ISerializer {
  def serialize(records: List[Result], baseFilename: String): ISerializer.Serialized

  val logger = LoggerFactory.getLogger(getClass)

  def serializeRecord[T](
    record: RawRecord,
    serializer: T,
    serialize: T => Unit
  ): Result =
    try {
      serialize(serializer)
      record.asRight
    } catch {
      case e: IOException =>
        val failure = GenericFailure(
          Instant.now(),
          NonEmptyList.one(s"IO error writing raw event to output stream. ${e}")
        )
        val payload = RawPayload(
          new String(Base64.encodeBase64(record), "UTF-8")
        )
        GenericError(S3Loader.processor, failure, payload).asLeft
      case NonFatal(e) =>
        logger.warn("Error writing raw event to output stream", e)
        val failure = GenericFailure(
          Instant.now(),
          NonEmptyList.one(s"Error writing raw event to output stream. ${e}")
        )
        val payload = RawPayload(
          new String(Base64.encodeBase64(record), "UTF-8")
        )
        GenericError(S3Loader.processor, failure, payload).asLeft
    }
}

object ISerializer {

  /** Pair of (file)name and its lazy content */
  case class NamedStream(filename: String, stream: ByteArrayOutputStream)

  /** Final list of created [[NamedStream]]s and rows being written */
  case class Serialized(namedStreams: List[NamedStream], results: List[Result])
}
