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

import java.time.Instant
import org.specs2.mutable.Specification

class DynamicPathSpec extends Specification {

  "DynamicPathSpec" should {
    val file = "bar.gz"
    val time = Instant.ofEpochMilli(100000L)

    "correctly decorate Time formats with one time pattern" in {
      val path = s"something/{YYYY}/$file"
      val actual = DynamicPath.decorateDirectoryWithTime(path, time)
      actual must_== s"something/1970/bar.gz"
    }

    "correctly decorate Time formats with multiple time patterns" in {
      val path = s"something/{YYYY}/{mm}dy={dd}/$file"
      val actual = DynamicPath.decorateDirectoryWithTime(path, time)
      actual must_== "something/1970/01dy=01/bar.gz"
    }

    "correctly decorate Time formats with extra trailing slash" in {
      val path = s"something/{YYYY}/{mm}dy={dd}/$file"
      val actual = DynamicPath.decorateDirectoryWithTime(path, time)
      actual must_== "something/1970/01dy=01/bar.gz"
    }

    "correctly decorate Time formats with invalid time format" in {
      val path = s"something/{YYYY}/{mm}dy={dd}/{foo}/$file"
      val actual = DynamicPath.decorateDirectoryWithTime(path, time)
      actual must_== "something/1970/01dy=01/foo/bar.gz"
    }

    "correctly handle no format" in {
      val actual = DynamicPath.decorateDirectoryWithTime(file, time)
      actual must_== "bar.gz"
    }

  }
}
