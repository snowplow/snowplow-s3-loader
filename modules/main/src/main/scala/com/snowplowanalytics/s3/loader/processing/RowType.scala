/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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
