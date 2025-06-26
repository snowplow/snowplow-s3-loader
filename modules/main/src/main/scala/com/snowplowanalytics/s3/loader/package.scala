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

package com.snowplowanalytics.s3

import com.snowplowanalytics.snowplow.badrows.BadRow.GenericError

package object loader {

  /**
   * Type for a RawRecord
   */
  type RawRecord = Array[Byte]

  /**
   * Final result of S3 Loader processing
   */
  type Result = Either[GenericError, RawRecord]
}
