/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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

import scalaz._

import org.specs2.mutable.Specification

class KinesisS3EmitterSpec extends Specification {

  "KinesisS3Emitter" should {

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
        Success(dataType11),
        Success(dataType21),
        Success(dataType22),
        Success(dataType31),
        Success(dataType32),
        Success(dataType33),
        Failure(failure),
        Failure(failure),
        Success(nonSelfDescribingJson),
        Success(nonJsonData)
      )

      val res = KinesisS3Emitter.partitionWithSchemaKey(records, "partition_error_dir")
        .map { case (rowType, l) => (rowType.getName, l)}

      val expected = Map(
        "com.acme1.example1" -> List(Success(dataType11)),
        "com.acme1.example2" -> List(Success(dataType21), Success(dataType22)),
        "com.acme2.example1" -> List(Success(dataType31), Success(dataType32), Success(dataType33)),
        "partition_error_dir" -> List(Success(nonSelfDescribingJson), Success(nonJsonData)),
        "unexpected_error" -> List(Failure(failure), Failure(failure))
      )

      res must beEqualTo(expected)
    }
  }

}
