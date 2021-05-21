package com.snowplowanalytics.s3.loader.processing

import java.time.Instant

import com.snowplowanalytics.s3.loader.EmitterInput
import com.snowplowanalytics.s3.loader.processing.Batch.Meta

/** Content of a KCL buffer with metadata attached */
final case class Batch[A](meta: Meta, data: A) {
  def map[B](f: A => B): Batch[B] =
    Batch(meta, f(data))
}

object Batch {

  type Partitioned = Batch[List[(RowType, List[EmitterInput])]]

  case class Meta(earliestTstamp: Option[Instant], count: Int) {
    def isEmpty: Boolean = earliestTstamp.isEmpty && count == 0
  }

  val EmptyMeta: Meta = Meta(None, 0)

  def fromEnriched(inputs: List[EmitterInput]): Batch[List[EmitterInput]] = {
    val earliest = Common.getEarliestTstamp(inputs)
    val count = inputs.length
    Batch(Meta(earliest, count), inputs)
  }

  def from(inputs: List[EmitterInput]): Batch[List[EmitterInput]] =
    Batch(EmptyMeta, inputs)
}