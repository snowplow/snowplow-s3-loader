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

import org.joda.time.DateTime
import org.specs2.mutable.Specification


class DynamicPathSpec extends Specification {

  "DynamicPathSpec" should {
    val file = "bar.gz"
    val time = new DateTime(100000)

    "correctly decorate Time formats with one time pattern" in {
      val directoryPattern = Some("something/{YYYY}")
      val actual = DynamicPath.decorateDirectoryWithTime(directoryPattern, file, time)
      actual must_== "something/1970/bar.gz"
    }

    "correctly decorate Time formats with multiple time patterns" in {
      val directoryPattern = Some("something/{YYYY}/{mm}dy={dd}")
      val actual = DynamicPath.decorateDirectoryWithTime(directoryPattern, file, time)
      actual must_== "something/1970/01dy=01/bar.gz"
    }

    "correctly decorate Time formats with extra trailing slash" in {
      val directoryPattern = Some("something/{YYYY}/{mm}dy={dd}/")
      val actual = DynamicPath.decorateDirectoryWithTime(directoryPattern, file, time)
      actual must_== "something/1970/01dy=01/bar.gz"
    }

    "correctly decorate Time formats with invalid time format" in {
      val directoryPattern = Some("something/{YYYY}/{mm}dy={dd}/{foo}")
      val actual = DynamicPath.decorateDirectoryWithTime(directoryPattern, file, time)
      actual must_== "something/1970/01dy=01/foo/bar.gz"
    }

    "correctly handle no format" in {
      val directoryPattern = None
      val actual = DynamicPath.decorateDirectoryWithTime(directoryPattern, file, time)
      actual must_== "bar.gz"
    }

  }
}
