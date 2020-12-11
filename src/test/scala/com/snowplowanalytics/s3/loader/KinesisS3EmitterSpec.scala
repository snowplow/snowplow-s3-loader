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
package com.snowplowanalytics.s3.loader

import java.time.LocalDateTime

import cats.syntax.validated._

import org.specs2.mutable.Specification

class KinesisS3EmitterSpec extends Specification {
  "KinesisS3Emitter" should {
    val firstSeq = "firstSeq"
    val lastSeq = "lastSeq"
    val partition = "com.snowplow.partition"
    val outputDirectory = "outputDirectory"
    val dateFormat = "{YYYY}/{MM}/{dd}/{HH}"
    val filenamePrefix = "fileNamePrefix"
    val datetime = LocalDateTime.of(1970, 1, 1, 0, 0)

    "format file name with optional components" in {
      val actual = KinesisS3Emitter.getBaseFilename(firstSeq,
                                                    lastSeq,
                                                    Some(outputDirectory),
                                                    Some(partition),
                                                    Some(dateFormat),
                                                    Some(filenamePrefix),
                                                    Some(datetime)
      )

      actual must beEqualTo(s"$outputDirectory/$partition/$dateFormat/$filenamePrefix-1970-01-01T00:00:00-$firstSeq-$lastSeq")
    }

    "format file name without optional components" in {
      val actual = KinesisS3Emitter.getBaseFilename(firstSeq, lastSeq, None, None, None, None, Some(datetime))

      actual must beEqualTo(s"1970-01-01T00:00:00-$firstSeq-$lastSeq")
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
