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
package com.snowplowanalytics.s3.loader.connector

import org.specs2.mutable.Specification

import com.snowplowanalytics.s3.loader.Config.{Compression, S3Output}

class KinesisS3EmitterSpec extends Specification {
  "KinesisS3Emitter" should {
    val firstSeq = "firstSeq"
    val lastSeq = "lastSeq"
    val partition = "com.snowplow.partition"
    val outputDirectory = "outputDirectory"
    val dateFormat = "{YYYY}/{MM}/{dd}/{HH}"
    val filenamePrefix = "fileNamePrefix"

    "format file name with optional components" in {
      val s3Config =
        S3Output(s"s3://no-bucket/$outputDirectory", Some(dateFormat), Some(filenamePrefix), Compression.Gzip, 0, None)
      val actual = KinesisS3Emitter.getBaseFilename(s3Config, firstSeq, lastSeq)(Some(partition))

      actual.replaceAll("\\d{4}-\\d{2}-\\d{2}-\\d{6}", "2021-04-30-000000") must beEqualTo(
        s"$outputDirectory/$partition/$dateFormat/$filenamePrefix-2021-04-30-000000-$firstSeq-$lastSeq"
      )
    }

    "format file name without optional components" in {
      val s3Config =
        S3Output("s3://no-bucket", None, None, Compression.Gzip, 0, None)
      val actual =
        KinesisS3Emitter.getBaseFilename(s3Config, firstSeq, lastSeq)(None)

      actual.replaceAll("\\d{4}-\\d{2}-\\d{2}-\\d{6}", "2021-04-30-000000") must beEqualTo(
        s"2021-04-30-000000-$firstSeq-$lastSeq"
      )
    }

    "format file name with path, but without optional components" in {
      val s3Config = S3Output(s"s3://no-bucket/$outputDirectory", None, None, Compression.Gzip, 0, None)
      val actual =
        KinesisS3Emitter.getBaseFilename(s3Config, firstSeq, lastSeq)(None)

      actual.replaceAll("\\d{4}-\\d{2}-\\d{2}-\\d{6}", "2021-04-30-000000") must beEqualTo(
        s"$outputDirectory/2021-04-30-000000-$firstSeq-$lastSeq"
      )
    }

    "format file name with path and partition" in {
      val s3Config = S3Output(s"s3://no-bucket/$outputDirectory", None, None, Compression.Gzip, 0, None)
      val actual = KinesisS3Emitter.getBaseFilename(s3Config, firstSeq, lastSeq)(Some("partition"))

      actual.replaceAll("\\d{4}-\\d{2}-\\d{2}-\\d{6}", "2021-04-30-000000") must beEqualTo(
        s"$outputDirectory/partition/2021-04-30-000000-$firstSeq-$lastSeq"
      )
    }
  }
}
