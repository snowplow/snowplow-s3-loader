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
package com.snowplowanalytics.s3.loader.connector

import java.time.LocalDateTime

import org.specs2.mutable.Specification

import com.snowplowanalytics.s3.loader.processing.RowType
import com.snowplowanalytics.s3.loader.Config.{Compression, Purpose, S3Output}

class KinesisS3EmitterSpec extends Specification {
  "KinesisS3Emitter" should {
    val firstSeq = "firstSeq"
    val lastSeq = "lastSeq"
    val sdj = RowType.SelfDescribing("com.snowplow", "myschema", "jsonschema", 42)
    val outputDirectory = "outputDirectory"
    val sdjPartitionFormat = "{vendor}.{schema}/model={model}/date={yy}-{mm}-{dd}"
    val rawPartitionFormat = "date={yy}-{mm}-{dd}"
    val filenamePrefix = "fileNamePrefix"
    val now = LocalDateTime.of(2020, 2, 4, 6, 8, 10, 12)

    "format file name for raw with optional components" in {
      val purpose = Purpose.Raw
      val s3Config =
        S3Output(s"s3://no-bucket/$outputDirectory", Some(rawPartitionFormat), Some(filenamePrefix), Compression.Gzip, 0, None)
      val actual = KinesisS3Emitter.getBaseFilename(s3Config, purpose, firstSeq, lastSeq, now)(Some(sdj))

      actual must beEqualTo(
        s"$outputDirectory/date=2020-02-04/$filenamePrefix-2020-02-04-060810-$firstSeq-$lastSeq"
      )
    }

    "format file name for sdj with optional components" in {
      val purpose = Purpose.SelfDescribingJson
      val s3Config =
        S3Output(s"s3://no-bucket/$outputDirectory", Some(sdjPartitionFormat), Some(filenamePrefix), Compression.Gzip, 0, None)
      val actual = KinesisS3Emitter.getBaseFilename(s3Config, purpose, firstSeq, lastSeq, now)(Some(sdj))

      actual must beEqualTo(
        s"$outputDirectory/com.snowplow.myschema/model=42/date=2020-02-04/$filenamePrefix-2020-02-04-060810-$firstSeq-$lastSeq"
      )
    }

    "format file name for raw without optional components" in {
      val purpose = Purpose.Raw
      val s3Config =
        S3Output("s3://no-bucket", None, None, Compression.Gzip, 0, None)
      val actual =
        KinesisS3Emitter.getBaseFilename(s3Config, purpose, firstSeq, lastSeq, now)(None)

      actual must beEqualTo(
        s"2020-02-04-060810-$firstSeq-$lastSeq"
      )
    }

    "format file name for sdj without optional components" in {
      val purpose = Purpose.SelfDescribingJson
      val s3Config =
        S3Output("s3://no-bucket", None, None, Compression.Gzip, 0, None)
      val actual =
        KinesisS3Emitter.getBaseFilename(s3Config, purpose, firstSeq, lastSeq, now)(None)

      actual must beEqualTo(
        s"unknown.unknown/2020-02-04-060810-$firstSeq-$lastSeq"
      )
    }

    "format file name with path, but without optional components" in {
      val purpose = Purpose.Raw
      val s3Config = S3Output(s"s3://no-bucket/$outputDirectory", None, None, Compression.Gzip, 0, None)
      val actual =
        KinesisS3Emitter.getBaseFilename(s3Config, purpose, firstSeq, lastSeq, now)(None)

      actual must beEqualTo(
        s"$outputDirectory/2020-02-04-060810-$firstSeq-$lastSeq"
      )
    }

    "format file name with path and partition" in {
      val purpose = Purpose.SelfDescribingJson
      val s3Config = S3Output(s"s3://no-bucket/$outputDirectory", None, None, Compression.Gzip, 0, None)
      val actual = KinesisS3Emitter.getBaseFilename(s3Config, purpose, firstSeq, lastSeq, now)(Some(sdj))

      actual must beEqualTo(
        s"$outputDirectory/com.snowplow.myschema/2020-02-04-060810-$firstSeq-$lastSeq"
      )
    }
  }
}
