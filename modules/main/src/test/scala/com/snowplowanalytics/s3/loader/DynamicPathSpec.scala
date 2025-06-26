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
