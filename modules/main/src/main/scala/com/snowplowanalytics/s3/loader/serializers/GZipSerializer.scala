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
package com.snowplowanalytics.s3.loader
package serializers

// Java libs
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

/**
 * Object to handle ZIP compression of raw events
 */
object GZipSerializer extends ISerializer {
  def serialize(records: List[Result], baseFilename: String): ISerializer.Serialized = {
    val outputStream = new ByteArrayOutputStream()
    val gzipOutputStream = new GZIPOutputStream(outputStream, 64 * 1024)

    // Populate the output stream with records
    val results = records.map {
      case Right(r) =>
        serializeRecord(
          r,
          gzipOutputStream,
          (g: GZIPOutputStream) => {
            g.write(r)
            g.write("\n".getBytes)
          }
        )
      case f => f
    }

    gzipOutputStream.close()

    val namedStreams = List(
      ISerializer.NamedStream(s"$baseFilename.gz", outputStream)
    )

    ISerializer.Serialized(namedStreams, results)
  }
}
