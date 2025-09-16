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

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload, Processor}

import java.nio.charset.StandardCharsets
import java.time.Instant

object BadRowsSerializer {

  /**
   * If input bad row exceeds provided max size in bytes, return serialized SizeViolation bad row
   * with trimmed original payload. If not, return original serialized bad row.
   */
  def withMaxSize(
    badRow: BadRow,
    processor: Processor,
    maxSize: Int
  ): Array[Byte] = {
    val originalSerialized = badRow.compactByteArray
    val originalSizeBytes  = originalSerialized.length

    if (originalSizeBytes >= maxSize) {
      val trimmedPayload = new String(originalSerialized, 0, maxSize / 10, StandardCharsets.UTF_8)
      BadRow
        .SizeViolation(
          processor,
          Failure.SizeViolation(Instant.now(), maxSize, originalSizeBytes, "Bad row exceeds allowed max size"),
          Payload.RawPayload(trimmedPayload)
        )
        .compactByteArray
    } else {
      originalSerialized
    }
  }
}
