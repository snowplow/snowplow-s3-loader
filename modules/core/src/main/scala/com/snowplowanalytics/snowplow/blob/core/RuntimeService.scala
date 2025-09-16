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

import cats.Show

sealed trait RuntimeService

object RuntimeService {
  case object BadSink extends RuntimeService
  case object BlobSink extends RuntimeService

  implicit val show: Show[RuntimeService] = Show.show {
    case BadSink  => "Bad rows sink"
    case BlobSink => "Blob sink"
  }
}
