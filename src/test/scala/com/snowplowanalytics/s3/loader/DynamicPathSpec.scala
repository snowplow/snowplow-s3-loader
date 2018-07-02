package com.snowplowanalytics.s3.loader

import org.joda.time.DateTime
import org.specs2.mutable.Specification


class DynamicPathSpec extends Specification {

  "DynamicPathSpec" should {
    val file = "bar.gz"
    val time = new DateTime(100000)

    "correctly decorate Time formats with one time pattern" in {
      val directoryPattern = "something/{YYYY}"
      val actual = DynamicPath.decorateDirectoryWithTime(directoryPattern, file, time)
      actual must_== "something/{1970}/bar.gz"
    }

    "correctly decorate Time formats with multiple time patterns" in {
      val directoryPattern = "something/{YYYY}/{mm}dy={dd}"
      val actual = DynamicPath.decorateDirectoryWithTime(directoryPattern, file, time)
      actual must_== "something/{1970}/{01}dy={01}/bar.gz"
    }

    "correctly decorate Time formats with extra trailing slash" in {
      val directoryPattern = "something/{YYYY}/{mm}dy={dd}/"
      val actual = DynamicPath.decorateDirectoryWithTime(directoryPattern, file, time)
      actual must_== "something/{1970}/{01}dy={01}/bar.gz"
    }

    "correctly decorate Time formats with invalid time format" in {
      val directoryPattern = "something/{YYYY}/{mm}dy={dd}/{foo}"
      val actual = DynamicPath.decorateDirectoryWithTime(directoryPattern, file, time)
      actual must_== "something/{1970}/{01}dy={01}/{foo}/bar.gz"
    }

  }
}
