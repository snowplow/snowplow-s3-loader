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

/** Type of row which determined according to schema of self describing data */
sealed trait RowType extends Product with Serializable

object RowType {

  /**
   * Old-school TSV/raw line that is not partitioned
   * Depending on partitioning setting should be either sank or sent to bad stream
   */
  case object Unpartitioned extends RowType

  /** JSON line with self-describing payload that can be partitioned */
  final case class SelfDescribing(vendor: String, name: String, format: String, model: Int) extends RowType

  /** Unrecognized line, e.g. non-string or non-SDJSON whereas partitioning is enabled */
  case object ReadingError extends RowType
}
