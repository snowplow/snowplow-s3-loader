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
package com.snowplowanalytics.s3.loader.processing

import java.time.Instant

import com.snowplowanalytics.s3.loader.Result
import com.snowplowanalytics.s3.loader.processing.Batch.Meta

/** Content of a KCL buffer with metadata attached */
final case class Batch[A](meta: Meta, data: A) {
  def map[B](f: A => B): Batch[B] =
    Batch(meta, f(data))
}

object Batch {

  type Partitioned = Batch[List[(RowType, List[Result])]]

  case class Meta(earliestTstamp: Option[Instant], count: Int) {
    def isEmpty: Boolean = earliestTstamp.isEmpty && count == 0
  }

  val EmptyMeta: Meta = Meta(None, 0)

  def fromEnriched(inputs: List[Result]): Batch[List[Result]] = {
    val earliest = Common.getEarliestTstamp(inputs)
    val count = inputs.length
    Batch(Meta(earliest, count), inputs)
  }

  def from(inputs: List[Result]): Batch[List[Result]] =
    Batch(EmptyMeta, inputs)
}