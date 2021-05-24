/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
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

import com.snowplowanalytics.s3.loader.{ S3Loader, Result, RawRecord }

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
        val failure = GenericFailure(Instant.now(), NonEmptyList.one(s"IO error writing raw event to output stream. ${e}"))
        val payload = RawPayload(new String(Base64.encodeBase64(record), "UTF-8"))
        GenericError(S3Loader.processor, failure, payload).asLeft
      case NonFatal(e) =>
        logger.warn("Error writing raw event to output stream", e)
        val failure = GenericFailure(Instant.now(), NonEmptyList.one(s"Error writing raw event to output stream. ${e}"))
        val payload = RawPayload(new String(Base64.encodeBase64(record), "UTF-8"))
        GenericError(S3Loader.processor, failure, payload).asLeft
    }
}

object ISerializer {

  /** Pair of (file)name and its lazy content */
  case class NamedStream(filename: String, stream: ByteArrayOutputStream)

  /** Final list of created [[NamedStream]]s and rows being written */
  case class Serialized(namedStreams: List[NamedStream], results: List[Result])
}