/*
 * Copyright (c) 2014-2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.s3.loader
package serializers

// Java libs
import java.io.{ByteArrayOutputStream, IOException}

// Scalaz
import scalaz._
import Scalaz._

// Scala
import scala.util.control.NonFatal

// SLF4j
import org.slf4j.LoggerFactory

// Apache commons
import org.apache.commons.codec.binary.Base64

case class NamedStream(filename: String, stream: ByteArrayOutputStream)

case class SerializationResult(namedStreams: List[NamedStream], results: List[EmitterInput])

/**
 * Shared interface for all serializers
 */
trait ISerializer {
  def serialize(records: List[EmitterInput], baseFilename: String): SerializationResult

  val log = LoggerFactory.getLogger(getClass)

  def serializeRecord[T](
    record: RawRecord,
    serializer: T,
    serialize: T => Unit
  ): ValidatedRecord =
    try {
      serialize(serializer)
      record.success
    } catch {
      case e: IOException =>
        val base64Record = new String(Base64.encodeBase64(record), "UTF-8")
        FailedRecord(List(s"Error writing raw event to output stream: [$e]"), base64Record).failure
      case NonFatal(e) =>
        log.warn("Error writing raw event to output stream", e)
        FailedRecord(List(s"Error writing raw event to output stream: [$e]"), "").failure
    }
}
