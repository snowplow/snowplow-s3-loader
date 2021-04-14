/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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

import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification
import pureconfig.ConfigSource

class ConfigSpec extends Specification {
  "Config" should {
    "be parsed from a string" in {
      val config = ConfigFactory.parseString("""
        source = "kinesis"

        sink = "kinesis"

        aws {
          accessKey = "default"
          secretKey = "default"
        }

        kinesis {
          initialPosition = "TRIM_HORIZON"
          maxRecords = 5
          region = "eu-central-1"
          appName = "s3-loader-integration-test"
        }

        streams {
          inStreamName = "s3-loader-integration-test"
          outStreamName = "s3-loader-integration-test-failures"

          buffer {
            byteLimit = 2048
            recordLimit = 10
            timeLimit = 5000
          }
        }

        s3 {
          region = "eu-central-1"
          bucket = "s3-loader-integration-test/usual"
          bucketJson = "s3-loader-integration-test/partitioned"
          format = "gzip"
          maxTimeout = 2000
        }

        monitoring {
          snowplow {
            collectorUri = "http://snplow.acme.ru"
            collectorPort = 80
            appId = "s3"
            method = "POST"
          }
        }""")

      val expected = Config(
        Config.Source.Kinesis,
        Config.Sink.Kinesis,
        Config.AWS("default","default"),
        Config.Kinesis(Config.InitialPosition.TrimHorizon,5,"eu-central-1","s3-loader-integration-test",None,None),
        Config.StreamsConfig("s3-loader-integration-test","s3-loader-integration-test-failures",Config.Buffer(2048,10,5000)),
        Config.S3("eu-central-1","s3-loader-integration-test/usual",None,Config.Format.Gzip,2000,None,None,None,None),
        Some(Config.Monitoring(Config.SnowplowMonitoring("http://snplow.acme.ru",80,"s3","POST")))
      )

      val result = ConfigSource.fromConfig(config).load[Config]

      result must beRight(expected)
    }
  }
}
