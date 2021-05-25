/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.s3.loader.connector

import cats.syntax.either._

import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer

import org.slf4j.LoggerFactory

import com.snowplowanalytics.s3.loader.{Result, RawRecord}

/** No-op serializer */
class IdentityTransformer extends ITransformer[RawRecord, Result] {

  val log = LoggerFactory.getLogger(getClass)

  def toClass(record: Record): RawRecord = {
    log.debug(s"Converting record: [$record] to EmitterInput before adding it to the buffer")
    record.getData.array
  }

  def fromClass(record: RawRecord): Result =
    record.asRight
}
