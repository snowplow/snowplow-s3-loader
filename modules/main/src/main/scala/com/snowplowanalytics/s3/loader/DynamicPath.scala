/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd. All rights reserved.
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

import java.nio.file.Paths
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.util.Try

/**
 * Object to handle S3 dynamic path generation
 */
object DynamicPath {

  /**
   * Function to decorate the directory pattern with time components from a given DateTime, it returns a string with
   * the final file path decorated with Instant values as per the directory pattern supplied
   *
   * @param fileName          A string which may contain date patterns enclosed in {curly braces}.
   *                          These patterns conform to the DateTime Formatter symbols as described here -
   *                          https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
   * @param decoratorDateTime The time to be used for decorating the directory patterns with actual values
   */
  def decorateDirectoryWithTime(fileName: String, decoratorDateTime: Instant): String = {
    val detectBracesExpression = "\\{(.*?)}".r
    val replacements = detectBracesExpression
      .findAllMatchIn(fileName)
      .map(_.toString.trim)
      .map { str =>
        val cleaned = str.filterNot(c => c == '{' || c == '}')
        (
          str,
          Try(
            DateTimeFormatter
              .ofPattern(cleaned)
              .withZone(ZoneOffset.UTC)
              .format(decoratorDateTime)
          ).toOption.getOrElse(str)
        )
      }
      .toList

    val directoryWithBraces = replacements.foldLeft(fileName) { case (dir, (target, replacement)) =>
      dir.replace(target, replacement)
    }

    // Remove braces from the final pure directory path
    val pure = """\{([^}]*)}""".r
    val path = pure.replaceAllIn(directoryWithBraces, "$1")
    normalize(path)
  }

  def normalize(pathStr: String): String = Paths.get(pathStr).normalize.toString
}
