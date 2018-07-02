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
  def decorateDirectoryWithTime(directoryPattern: String, fileName: String, decoratorDateTime: DateTime): String = {
    // replace the pattern with actual date values
    val detectBracesExpression = "\\{(.*?)\\}".r
    val replacements = detectBracesExpression
      .findAllMatchIn(directoryPattern)
      .map(_.toString.trim)
      .map(str => {
        val converted = Try(DateTimeFormat.forPattern(str).withZone(DateTimeZone.UTC).print(decoratorDateTime))
        (str, converted.getOrElse(str))
      }
      )
      .toList
    val directory = replacements.foldLeft(directoryPattern) {
      (dir, replacement) => dir.replace(replacement._1, replacement._2)
    }
    // ensure trailing slash in the directory only if a pattern was supplied
    val finalPath = if (directory.endsWith("/") || directory.isEmpty) s"$directory$fileName"
    else s"$directory/$fileName"
    finalPath
  }
}

