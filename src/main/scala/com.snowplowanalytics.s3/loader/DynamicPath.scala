/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import scala.util.Try

/**
  * Object to handle S3 dynamic path generation
  */
object DynamicPath {
  /**
    * Function to decorate the directory pattern with time components from a given DateTime, it returns a string with
    * the final file path decorated with DateTime Values as per the directory pattern supplied
    *
    * @param directoryPattern  A string option which contains patterns enclosed in {curly braces}.
    *                          These patterns conform to the DateTime Formatter symbols as described here -
    *                          https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
    * @param fileName          Name of the file to be placed in the directory
    * @param decoratorDateTime The DateTime to be used for decorating the directory patterns with actual values
    */
  def decorateDirectoryWithTime(directoryPattern: Option[String], fileName: String, decoratorDateTime: DateTime): String = {
    directoryPattern match {
      case Some(pattern) =>
        // replace the pattern with actual date values
        val detectBracesExpression = "\\{(.*?)\\}".r
        val replacements = detectBracesExpression
          .findAllMatchIn(pattern)
          .map(_.toString.trim)
          .map { str =>
            (str, Try(DateTimeFormat.forPattern(str).withZone(DateTimeZone.UTC).print(decoratorDateTime)).getOrElse(str))
          }
          .toList
        val directoryWithBraces = replacements.foldLeft(pattern) {
          (dir, replacement) => dir.replace(replacement._1, replacement._2)
        }
        // Remove braces from the final pure directory path
        val pure = """\{([^}]*)\}""".r
        val directory = pure.replaceAllIn(directoryWithBraces, "$1")
        // ensure trailing slash in the directory only if a pattern was supplied
        val finalPath = if (directory.endsWith("/") || directory.isEmpty) s"$directory$fileName"
        else s"$directory/$fileName"
        finalPath
      case None => fileName
    }
  }
}

