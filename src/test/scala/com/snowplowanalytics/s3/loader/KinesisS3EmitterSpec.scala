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
import Scalaz._

import org.specs2.mutable.Specification
import com.snowplowanalytics.s3.loader.KinesisS3Emitter.RowType

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

      val res = KinesisS3Emitter.partitionByType(records)

      val expected = Map(
        RowType.SelfDescribing("com.acme1", "example1", "jsonschema", 2) -> List(dataType11.success),
        RowType.SelfDescribing("com.acme1", "example2", "jsonschema", 2) -> List(dataType21.success, dataType22.success),
        RowType.SelfDescribing("com.acme2", "example1", "jsonschema", 2) -> List(dataType31.success, dataType32.success, dataType33.success),
        RowType.ReadingError -> List(failure.failure, failure.failure),
        RowType.Unpartitioned -> List(nonSelfDescribingJson.success, nonJsonData.success)
      )

      res must beEqualTo(expected)
    }
  }

}
