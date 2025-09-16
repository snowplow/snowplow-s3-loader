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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPOutputStream

class CompressedStream(size: Int) extends ByteArrayOutputStream(size) {

  private val compressed = new GZIPOutputStream(this, true)

  def toByteBuffer: ByteBuffer = {
    compressed.close
    ByteBuffer.wrap(buf, 0, count)
  }

  def write(lines: List[String]): Unit = {
    if (count == CompressedStream.GZIP_HEADER_LENGTH) {
      lines match {
        case head :: tail =>
          compressed.write(head.getBytes(StandardCharsets.UTF_8))
          addLines(tail)
        case Nil => ()
      }
    } else
      addLines(lines)

    compressed.flush
  }

  private def addLines(lines: List[String]): Unit =
    lines.foreach { line =>
      compressed.write("\n".getBytes(StandardCharsets.UTF_8))
      compressed.write(line.getBytes(StandardCharsets.UTF_8))
    }
}

object CompressedStream {
  val GZIP_HEADER_LENGTH = 10
}
