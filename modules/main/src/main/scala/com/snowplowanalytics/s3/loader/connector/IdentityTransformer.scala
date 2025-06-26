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
package com.snowplowanalytics.s3.loader.connector

import cats.syntax.either._

import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer

import org.slf4j.LoggerFactory

import com.snowplowanalytics.s3.loader.{RawRecord, Result}

/** No-op serializer */
class IdentityTransformer extends ITransformer[RawRecord, Result] {

  val log = LoggerFactory.getLogger(getClass)

  def toClass(record: Record): RawRecord = {
    log.debug(
      s"Converting record: [$record] to EmitterInput before adding it to the buffer"
    )
    record.getData.array
  }

  def fromClass(record: RawRecord): Result =
    record.asRight
}
