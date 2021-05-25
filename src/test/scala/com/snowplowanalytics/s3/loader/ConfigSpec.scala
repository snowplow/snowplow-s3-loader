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

import java.net.URI
import java.nio.file.Paths

import com.typesafe.config.ConfigFactory

import pureconfig.ConfigSource

import org.specs2.mutable.Specification

import com.snowplowanalytics.s3.loader.Config.{Compression, InitialPosition, Purpose, S3Output}

class ConfigSpec extends Specification {
  "Config" should {
    "be parsed from a string" in {
      val config = ConfigFactory.parseString(
        """{
        "region": "eu-central-1",
        "purpose": "raw",

        "input": {
            "appName": "acme-s3-loader",
            "streamName": "enriched-events",
            "position": "LATEST",
            "maxRecords": 10
        },

        "output": {
            "s3": {
                "path": "s3://s3-loader-integration-test/usual",

                "dateFormat": "%Y-%M-%d",
                "filenamePrefix": "pre",

                "maxTimeout": 2000,
                "compression": "gzip"
            },

            "bad": {
              "streamName": "stream-name"
            }
        },

        "buffer": {
            "byteLimit": 2048,
            "recordLimit": 10,
            "timeLimit": 5000
        },

        "monitoring": {
            "snowplow": {
                "collector": "http://snplow.acme.ru",
                "appId": "angry-birds"
            },
        }
    }""")

      val expected = Config(
        Some("eu-central-1"),
        Purpose.Raw,
        Config.Input("acme-s3-loader", "enriched-events", InitialPosition.Latest, None, 10),
        Config.Output(S3Output("s3://s3-loader-integration-test/usual", Some("%Y-%M-%d"), Some("pre"), Compression.Gzip, 2000, None), Config.KinesisOutput("stream-name")),
        Config.Buffer(2048L, 10L, 5000L),
        Some(
          Config.Monitoring(
            Some(
              Config.SnowplowMonitoring(URI.create("http://snplow.acme.ru"), "angry-birds")
            ),
            None,
            None
          )
        )
      )

      val result = ConfigSource.fromConfig(config).load[Config]

      result must beRight(expected)
    }
  }

  "Config.load" should {
    "parse the config from an example file" in {
      val configPath =
        Paths.get(getClass.getResource("/config.hocon.sample").toURI)

      val expected = Config(
        Some("eu-central-1"),
        Purpose.Raw,
        Config.Input("acme-s3-loader", "raw-events", InitialPosition.Latest, None, 10),
        Config.Output(S3Output("s3://acme-snowplow-output/raw/", Some("%Y-%M-%d"), Some("pre"), Compression.Gzip, 2000, None), Config.KinesisOutput("stream-name")),
        Config.Buffer(2048L, 10L, 5000L),
        Some(Config.Monitoring(
          Some(Config.SnowplowMonitoring(URI.create("http://snplow.acme.ru:80"), "angry-birds")),
          Some(Config.Sentry(URI.create("https://sentry.acme.com/42"))),
          Some(Config.Metrics(Some(false), Some(Config.StatsD("statsd.acme.ru", 1024, Map.empty, Some("snowplow.monitoring")))))
        ))
      )

      Config.load(configPath) must beRight(expected)
    }

    "provide a human-readable error" in {
      val configPath =
        Paths.get(getClass.getResource("/config.invalid").toURI)

      Config.load(configPath) must be like {
        case Left(s) => s must contain("DecodingFailure at .input.appName: Attempt to decode value on failed cursor")
        case Right(_) => ko("Decoding succeeded with invalid config")
      }
    }
  }
}
