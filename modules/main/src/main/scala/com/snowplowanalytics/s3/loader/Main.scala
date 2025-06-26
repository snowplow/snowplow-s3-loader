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

// Java
import java.nio.file.Path

// Decline
import com.monovore.decline._

import cats.syntax.show._

/**
 * The entrypoint class for the Kinesis-S3 Sink application.
 */
trait MainPlatform {

  val config = Opts
    .option[Path]("config", "Path to configuration HOCON file", "c", "filename")
  val parser =
    Command(s"${generated.Settings.name}-${generated.Settings.version}", "Streaming sink app for S3")(config)

  def withConfig(args: Array[String])(f: Config => Unit): Unit =
    parser.parse(args.toList) match {
      case Right(c) =>
        Config.load(c) match {
          case Right(config) =>
            f(config)
          case Left(e) =>
            System.err.println(s"Configuration error: $e")
            System.exit(1)
        }
      case Left(error) =>
        System.err.println(error.show)
        System.exit(1)
    }
}

object Main extends MainPlatform {
  def main(args: Array[String]): Unit =
    withConfig(args)(S3Loader.run)
}
