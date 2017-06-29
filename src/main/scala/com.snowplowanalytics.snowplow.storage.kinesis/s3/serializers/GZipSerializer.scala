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

// Java libs
import java.io.ByteArrayOutputStream

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
    val results = records.map { v =>
      v match {
        case Success(r) => serializeRecord(r, gzipOutputStream,
          (g: GZIPOutputStream) => {
            g.write(r)
            g.write("\n".getBytes)
          })
        case f => f
      }
    }

    gzipOutputStream.close

    val namedStreams = List(NamedStream(s"$baseFilename.gz", outputStream))

    SerializationResult(namedStreams, results)
  }
}

