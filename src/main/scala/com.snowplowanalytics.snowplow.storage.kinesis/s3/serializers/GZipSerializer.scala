/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.kinesis.s3.serializers

import scala.collection.JavaConverters._

import com.snowplowanalytics.snowplow.storage.kinesis.s3._

// Scalaz
import scalaz._
import Scalaz._

// Apache commons
import org.apache.commons.codec.binary.Base64

// Java libs
import java.io.{
  ByteArrayOutputStream,
  IOException
}

// Logging
import org.apache.commons.logging.LogFactory

import java.util.zip.GZIPOutputStream

/**
 * Object to handle ZIP compression of raw events
 */
object GZipSerializer extends ISerializer {
  def serialize(records: List[ EmitterInput ], baseFilename: String): SerializationResult = {
    val log = LogFactory.getLog(getClass)

    val outputStream = new ByteArrayOutputStream()
    val gzipOutputStream = new GZIPOutputStream(outputStream, 64 * 1024)

    // Populate the output stream with records
    // TODO: Should there be a check for failures?
    val results = for { 
      Success(record) <- records 
    } yield {
      try {
        gzipOutputStream.write(record)
        gzipOutputStream.write("\n".getBytes)
        record.success
      } catch {
        case e: IOException => {
          log.warn(e)
          val base64Record = new String(Base64.encodeBase64(record), "UTF-8")
          FailedRecord(List("Error writing raw event to output stream: [%s]".format(e.toString)), base64Record).fail
        }

        // Need to log OutOfMemoryErrors
        case t: Throwable => {
          log.error("Error writing raw stream to output stream", t)
          throw t
        }
      }
    }

    gzipOutputStream.close

    val namedStreams = List(NamedStream(s"$baseFilename.gz", outputStream))

    SerializationResult(namedStreams, results)
  }
}

