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
import java.nio.ByteBuffer

trait BlobSink[F[_]] {
  def write(uri: URI, events: ByteBuffer): F[Unit]

  def write(uri: URI, events: CompressedStream): F[Unit] =
    write(uri, events.toByteBuffer)
}
