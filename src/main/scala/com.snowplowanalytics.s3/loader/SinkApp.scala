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

// Java
import java.io.File

// Config
import com.typesafe.config.ConfigFactory

// Pureconfig
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

import model._

/**
 * The entrypoint class for the Kinesis-S3 Sink applciation.
 */
object SinkApp {

  private case class FileConfig(config: File)

  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[FileConfig](generated.Settings.name) {
      head(generated.Settings.name, generated.Settings.version)
      opt[File]("config").required().valueName("<filename>")
        .action((f: File, c: FileConfig) => c.copy(f))
        .validate(f =>
          if (f.exists) success
          else failure(s"Configuration file $f does not exist")
        )
    }

    parser.parse(args, FileConfig(new File("."))) match {
      case Some(c) =>
        val config = ConfigFactory.parseFile(c.config).resolve()
        ConfigSource.fromConfig(config).load[S3LoaderConfig] match {
          case Right(config) =>
            S3Loader.run(config)
          case Left(e) =>
            System.err.println(s"Configuration error: $e")
            System.exit(1)
        }
      case None =>
        System.err.println(s"No config provided")
        System.exit(1)
    }
  }
}
