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

import cats.data.NonEmptyList
import cats.syntax.either._

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.badrows.BadRow.GenericError
import com.snowplowanalytics.snowplow.badrows.Failure.GenericFailure
import com.snowplowanalytics.snowplow.badrows.Payload.RawPayload

import com.snowplowanalytics.s3.loader.{Config, Result, S3Loader}

class CommonSpec extends Specification {
  "partitionByType" should {
    "partition records correctly according to schema key" in {
      import CommonSpec._

      val records = List(
        dataType11.asRight,
        dataType21.asRight,
        dataType22.asRight,
        dataType31.asRight,
        dataType32.asRight,
        dataType33.asRight,
        failedRecord.asLeft,
        failedRecord.asLeft,
        nonSelfDescribingJson.asRight,
        nonJsonData.asRight
      )

      val res = Common.partitionByType(records)

      val expected = Map(
        RowType.SelfDescribing("com.acme1", "example1", "jsonschema", 2) -> List(dataType11.asRight),
        RowType.SelfDescribing("com.acme1", "example2", "jsonschema", 2) -> List(dataType21.asRight, dataType22.asRight),
        RowType.SelfDescribing("com.acme2", "example1", "jsonschema", 2) -> List(
          dataType31.asRight,
          dataType32.asRight,
          dataType33.asRight
        ),
        RowType.ReadingError -> List(failedRecord.asLeft, failedRecord.asLeft),
        RowType.Unpartitioned -> List(nonSelfDescribingJson.asRight, nonJsonData.asRight)
      )

      res must beEqualTo(expected)
    }

    "handle empty list of records to partition" in {
      val records = List.empty[Result]
      val expected = Map.empty[RowType, List[Result]]
      val actual = Common.partitionByType(records)
      actual ==== expected
    }
  }

  "getTimestamp" should {
    "parse timestamp in proper format" in {
      val input = List.fill(4)("2020-11-26 00:01:05").mkString("\t")
      val expected = Instant.parse("2020-11-26T00:01:05Z")
      Common.getTstamp(input) must beRight(expected)
    }
  }

  "partition" should {
    "add metadata for enriched if statsd is enabled" in {
      val input = List("".getBytes.asRight)
      val result = Common.partition(Config.Purpose.Enriched, true, input)
      result.meta should beEqualTo(Batch.Meta(None, 1))
    }

    "not add metadata for enriched if statsd is disabled" in {
      val input = List("".getBytes.asRight)
      val result = Common.partition(Config.Purpose.Enriched, false, input)
      result.meta should beEqualTo(Batch.EmptyMeta)
    }

    "partition by type for self-describing" in {
      import CommonSpec._

      val input = List(dataType11.asRight, dataType21.asRight)
      val result =
        Common.partition(Config.Purpose.SelfDescribingJson, false, input)
      result should beEqualTo(
        Batch(
          Batch.EmptyMeta,
          List(
            (RowType.SelfDescribing("com.acme1", "example1", "jsonschema", 2), List(dataType11.asRight)),
            (RowType.SelfDescribing("com.acme1", "example2", "jsonschema", 2), List(dataType21.asRight))
          )
        )
      )
    }
  }
}

object CommonSpec {

  val dataType11 =
    """
      | {
      |   "schema": "iglu:com.acme1/example1/jsonschema/2-0-1",
      |   "data": "data1"
      | }
        """.stripMargin.getBytes("UTF-8")

  val dataType21 =
    """
      | {
      |   "schema": "iglu:com.acme1/example2/jsonschema/2-0-0",
      |   "data": "data1"
      | }
        """.stripMargin.getBytes("UTF-8")

  val dataType22 =
    """
      | {
      |   "schema": "iglu:com.acme1/example2/jsonschema/2-0-1",
      |   "data": "data2"
      | }
        """.stripMargin.getBytes("UTF-8")

  val dataType31 =
    """
      | {
      |   "schema": "iglu:com.acme2/example1/jsonschema/2-0-0",
      |   "data": "data1"
      | }
        """.stripMargin.getBytes("UTF-8")

  val dataType32 =
    """
      | {
      |   "schema": "iglu:com.acme2/example1/jsonschema/2-0-1",
      |   "data": "data2"
      | }
        """.stripMargin.getBytes("UTF-8")

  val dataType33 =
    """
      | {
      |   "schema": "iglu:com.acme2/example1/jsonschema/2-0-1",
      |   "data": "data3"
      | }
        """.stripMargin.getBytes("UTF-8")

  val nonSelfDescribingJson =
    """
      | {
      |   "data": "data",
      |   "key": "value"
      | }
        """.stripMargin.getBytes("UTF-8")

  val nonJsonData = "nonJsonData".getBytes("UTF-8")

  val failedRecord = GenericError(
    S3Loader.processor,
    GenericFailure(Instant.now(), NonEmptyList.one("error")),
    RawPayload("input")
  )
}
