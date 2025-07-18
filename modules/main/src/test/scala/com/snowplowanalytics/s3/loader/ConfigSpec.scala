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
      val config = ConfigFactory.parseString("""{
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

                "partitionFormat": "schema={vendor}.{schema}/year={yy}",
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
        Config.Output(
          S3Output("s3://s3-loader-integration-test/usual",
                   Some("schema={vendor}.{schema}/year={yy}"),
                   Some("pre"),
                   Compression.Gzip,
                   2000,
                   None),
          Config.KinesisOutput("stream-name")
        ),
        Config.Buffer(2048L, 10L, 5000L),
        Some(
          Config.Monitoring(
            Some(
              Config.SnowplowMonitoring(URI.create("http://snplow.acme.ru"), "angry-birds")
            ),
            None,
            None
          )
        ),
        None
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
        Config.Output(
          S3Output("s3://acme-snowplow-output/raw/",
                   Some("{vendor}.{schema}/model={model}/date={yy}-{mm}-{dd}"),
                   Some("pre"),
                   Compression.Gzip,
                   2000,
                   None),
          Config.KinesisOutput("stream-name")
        ),
        Config.Buffer(2048L, 10L, 5000L),
        Some(
          Config.Monitoring(
            Some(
              Config.SnowplowMonitoring(URI.create("http://snplow.acme.ru:80"), "angry-birds")
            ),
            Some(Config.Sentry(URI.create("https://sentry.acme.com/42"))),
            Some(
              Config.Metrics(
                Some(false),
                Some(
                  Config.StatsD("statsd.acme.ru", 1024, Map.empty, Some("snowplow.monitoring"))
                )
              )
            )
          )
        ),
        Some(Config.License(true))
      )

      Config.load(configPath) must beRight(expected)
    }

    "provide a human-readable error" in {
      val configPath =
        Paths.get(getClass.getResource("/config.invalid").toURI)

      Config.load(configPath) must be like {
        case Left(s)  => s must contain("DecodingFailure at .input.appName: Attempt to decode value on failed cursor")
        case Right(_) => ko("Decoding succeeded with invalid config")
      }
    }
  }
}
