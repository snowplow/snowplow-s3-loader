package com.snowplowanalytics.s3.loader.processing

import java.time.Instant
import java.nio.charset.StandardCharsets.UTF_8

import cats.data.Validated
import cats.syntax.either._

import io.circe.parser.parse

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.s3.loader.EmitterInput
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
  def partition(purpose: Purpose, statsDEnabled: Boolean, records: List[EmitterInput]): Batch.Partitioned =
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
  def partitionByType(records: List[EmitterInput]): Map[RowType, List[EmitterInput]] =
    records.groupBy {
      case Validated.Valid(byteRecord) =>
        val strRecord = new String(byteRecord, UTF_8)
        parse(strRecord) match {
          case Right(json) =>
            val schemaKey = SchemaKey.extract(json)
            schemaKey.fold(_ => RowType.Unpartitioned, k => RowType.SelfDescribing(k.vendor, k.name, k.format, k.version.model))
          case _ => RowType.Unpartitioned
        }
      case _ => RowType.ReadingError
    }

  /**
   *  Extract the earliest timestamp from a batch of payloads
   */
  def getEarliestTstamp(records: List[EmitterInput]): Option[Instant] = {
    val timestamps = records.flatMap {
      case Validated.Valid(byteRecord) =>
        val strRecord = new String(byteRecord, UTF_8)
        getTstamp(strRecord).toOption
      case Validated.Invalid(_) =>
        Nil
    }
    timestamps.sorted.headOption
  }

  /** Extract a timestamp from enriched TSV line */
  def getTstamp(row: String): Either[RuntimeException, Instant] = {
    val array = row.split("\t", -1)
    for {
      string <- Either.catchOnly[IndexOutOfBoundsException](array(CollectorTstampIdx)).map(_.replaceAll(" ", "T") + "Z")
      tstamp <- Either.catchOnly[DateTimeParseException](Instant.parse(string))
    } yield tstamp
  }
}
