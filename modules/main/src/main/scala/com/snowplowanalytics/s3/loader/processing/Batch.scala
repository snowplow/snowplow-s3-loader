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
package com.snowplowanalytics.s3.loader.processing

import java.time.Instant
import java.nio.charset.StandardCharsets.UTF_8

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
    val meta = inputs.foldLeft(EmptyMeta) {
      case (Meta(tstamp, count), Left(_)) =>
        Meta(tstamp, count + 1)
      case (Meta(tstamp, count), Right(raw)) =>
        val strRecord = new String(raw, UTF_8)
        val extracted = Common.getTstamp(strRecord).toOption
        val min = Common.compareTstamps(tstamp, extracted)
        Meta(min, count + 1)
    }

    Batch(meta, inputs)
  }

  def from(inputs: List[Result]): Batch[List[Result]] =
    Batch(EmptyMeta, inputs)
}
