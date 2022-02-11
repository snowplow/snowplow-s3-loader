/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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

// Java
import java.nio.file.Path

// Decline
import com.monovore.decline._

import cats.syntax.show._

/**
 * The entrypoint class for the Kinesis-S3 Sink application.
 */
object Main {

  val config = Opts
    .option[Path]("config", "Path to configuration HOCON file", "c", "filename")
  val parser =
    Command(s"${generated.Settings.name}-${generated.Settings.version}", "Streaming sink app for S3")(config)

  def main(args: Array[String]): Unit =
    parser.parse(args.toList) match {
      case Right(c) =>
        Config.load(c) match {
          case Right(config) =>
            S3Loader.run(config)
          case Left(e) =>
            System.err.println(s"Configuration error: $e")
            System.exit(1)
        }
      case Left(error) =>
        System.err.println(error.show)
        System.exit(1)
    }
}
