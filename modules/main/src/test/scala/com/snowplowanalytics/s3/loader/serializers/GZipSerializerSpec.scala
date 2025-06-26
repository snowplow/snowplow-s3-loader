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
package com.snowplowanalytics.s3.loader.serializers

// Java
import java.io.{File, FileOutputStream}
import java.nio.file.{FileSystems, Files}
import java.nio.charset.Charset

// cats
import cats.syntax.either._

// Scala
import scala.sys.process._
import scala.jdk.CollectionConverters._

// Specs2
import org.specs2.mutable.Specification

class GZipSerializerSpec extends Specification {

  "The GZipSerializer" should {
    "correctly serialize and compress a list of CollectorPayloads" in {
      val decompressedFilename = "/tmp/kinesis-s3-sink-test-gzip"

      val compressedFilename = decompressedFilename + ".gz"

      def cleanup() = List(compressedFilename, decompressedFilename).foreach(
        new File(_).delete()
      )

      cleanup()

      val binaryInputs = List(
        (List("A", "B", 1000, "a", "b"):List[Any]).mkString("\t").getBytes.asRight,
        (List("X", "Y", 2000, "x", "y"):List[Any]).mkString("\t").getBytes.asRight
      )

      val serializationResult =
        GZipSerializer.serialize(binaryInputs, decompressedFilename)

      val gzipOutput = serializationResult.namedStreams.head.stream

      gzipOutput.writeTo(new FileOutputStream(compressedFilename))

      s"gunzip $compressedFilename" !!

      val charset = Charset.forName("UTF-8")
      val path = FileSystems.getDefault().getPath(decompressedFilename)
      var input = Files.readAllLines(path, charset)

      cleanup()

      binaryInputs map {
        case Right(e) =>
          val rawResult = input.asScala.head.getBytes

          input = input.asScala.tail.asJava

          rawResult must_== e
        case _ =>
          1 must_== 2 // Should never happen
      }
    }
  }
}
