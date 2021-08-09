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
import java.nio.charset.StandardCharsets.UTF_8

import cats.syntax.either._

import io.circe.parser.parse

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.s3.loader.Result
import com.snowplowanalytics.s3.loader.Config.Purpose
import com.snowplowanalytics.s3.loader.monitoring.StatsD.CollectorTstampIdx

import java.time.format.DateTimeParseException

object Common {

  /**
   * Build a batch with metadata out of a list of records
   * Different metrics can be built depending on a `purpose` of the loader
   * For example, only enriched events would generate timestamp metrics
   * @param purpose the kind of a data the loader supposed to process
   * @param statsDEnabled whether any metrics should be reported at all
   * @param records raw records themselves
   */
  def partition(
    purpose: Purpose,
    statsDEnabled: Boolean,
    records: List[Result]
  ): Batch.Partitioned =
    purpose match {
      case Purpose.SelfDescribingJson =>
        Batch.from(records).map(rs => partitionByType(rs).toList)
      case Purpose.Enriched if statsDEnabled =>
        Batch.fromEnriched(records).map(rs => List((RowType.Unpartitioned, rs)))
      case _ =>
        Batch.from(records).map(rs => List((RowType.Unpartitioned, rs)))
    }

  /**
   * Assume records are self describing data and group them according
   * to their schema key. Put records which are not self describing data
   * to under "old bad row type".
   */
  def partitionByType(records: List[Result]): Map[RowType, List[Result]] =
    records.groupBy {
      case Right(byteRecord) =>
        val strRecord = new String(byteRecord, UTF_8)
        parse(strRecord) match {
          case Right(json) =>
            val schemaKey = SchemaKey.extract(json)
            schemaKey.fold(_ => RowType.Unpartitioned, k => RowType.SelfDescribing(k.vendor, k.name, k.format, k.version.model))
          case _ => RowType.Unpartitioned
        }
      case Left(_) => RowType.ReadingError
    }

  /** Extract a timestamp from enriched TSV line */
  def getTstamp(row: String): Either[RuntimeException, Instant] = {
    val array = row.split("\t", -1)
    for {
      string <- Either
                  .catchOnly[IndexOutOfBoundsException](array(CollectorTstampIdx))
                  .map(_.replaceAll(" ", "T") + "Z")
      tstamp <- Either.catchOnly[DateTimeParseException](Instant.parse(string))
    } yield tstamp
  }

  def compareTstamps(a: Option[Instant], b: Option[Instant]): Option[Instant] =
    (a, b) match {
      case (Some(ai), Some(bi)) => Some(if (ai.isBefore(bi)) ai else bi)
      case (None, bi @ Some(_)) => bi
      case (ai @ Some(_), None) => ai
      case _                    => None
    }
}
