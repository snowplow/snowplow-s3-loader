/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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

import cats.syntax.validated._

import com.snowplowanalytics.s3.loader.{EmitterInput, FailedRecord, RowType}
import com.snowplowanalytics.s3.loader.Config.{Format, S3}

import org.specs2.mutable.Specification


class KinesisS3EmitterSpec extends Specification {
  "KinesisS3Emitter" should {
    val firstSeq = "firstSeq"
    val lastSeq = "lastSeq"
    val partition = "com.snowplow.partition"
    val outputDirectory = "outputDirectory"
    val dateFormat = "{YYYY}/{MM}/{dd}/{HH}"
    val filenamePrefix = "fileNamePrefix"

    "format file name with optional components" in {
      val s3Config = S3("eu-central-1", "no-bucket", None, Format.Gzip, 0L, Some(outputDirectory), None, Some(dateFormat), Some(filenamePrefix))
      val actual = KinesisS3Emitter.getBaseFilename(s3Config, firstSeq, lastSeq)(Some(partition))

      actual.replaceAll("\\d{4}-\\d{2}-\\d{2}-\\d{6}", "2021-04-30-000000") must beEqualTo(s"$outputDirectory/$partition/$dateFormat/$filenamePrefix-2021-04-30-000000-$firstSeq-$lastSeq")
    }

    "format file name without optional components" in {
      val s3Config = S3("eu-central-1", "no-bucket", None, Format.Gzip, 0L, None, None, None, None)
      val actual = KinesisS3Emitter.getBaseFilename(s3Config, firstSeq, lastSeq)(None)

      actual.replaceAll("\\d{4}-\\d{2}-\\d{2}-\\d{6}", "2021-04-30-000000") must beEqualTo(s"2021-04-30-000000-$firstSeq-$lastSeq")
    }

    "partition records correctly according to schema key" in {
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

      val failure = FailedRecord(List("error1", "error2"), "line")

      val records = List(
        dataType11.valid,
        dataType21.valid,
        dataType22.valid,
        dataType31.valid,
        dataType32.valid,
        dataType33.valid,
        failure.invalid,
        failure.invalid,
        nonSelfDescribingJson.valid,
        nonJsonData.valid
      )

      val res = KinesisS3Emitter.partitionByType(records)

      val expected = Map(
        RowType.SelfDescribing("com.acme1", "example1", "jsonschema", 2) -> List(dataType11.valid),
        RowType.SelfDescribing("com.acme1", "example2", "jsonschema", 2) -> List(dataType21.valid, dataType22.valid),
        RowType.SelfDescribing("com.acme2", "example1", "jsonschema", 2) -> List(dataType31.valid, dataType32.valid, dataType33.valid),
        RowType.ReadingError -> List(failure.invalid, failure.invalid),
        RowType.Unpartitioned -> List(nonSelfDescribingJson.valid, nonJsonData.valid)
      )

      res must beEqualTo(expected)
    }

    "handle empty list of records to partition" in {
      val records = List.empty[EmitterInput]
      val expected = Map.empty[RowType, List[EmitterInput]]
      val actual = KinesisS3Emitter.partitionByType(records)
      actual ==== expected
    }
  }
}
