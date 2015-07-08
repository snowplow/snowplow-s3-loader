package com.snowplowanalytics.snowplow
package storage.kinesis.s3.serializers

// Java
import java.util.Properties
import java.io.{
  File,
  FileInputStream,
  FileOutputStream,
  BufferedInputStream
}

import java.nio.file.{
  FileSystems,
  Files
}

import java.nio.charset.Charset

// AWS libs
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  UnmodifiableBuffer
}
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer

// Elephant Bird
import com.twitter.elephantbird.mapreduce.io.RawBlockReader

// Scalaz
import scalaz._
import Scalaz._

// Scala
import scala.sys.process._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

class GZipSerializerSpec extends Specification with ValidationMatchers {

  "The GZipSerializer" should {
    "correctly serialize and compress a list of CollectorPayloads" in {
      val decompressedFilename = "/tmp/kinesis-s3-sink-test-gzip"

      val compressedFilename = decompressedFilename + ".gz"

      def cleanup() = List(compressedFilename, decompressedFilename).foreach(new File(_).delete())

      cleanup()

      val binaryInputs = List(
        List("A", "B", 1000, "a", "b").mkString("\t").getBytes.success,
        List("X", "Y", 2000, "x", "y").mkString("\t").getBytes.success
      )

      val serializationResult = GZipSerializer.serialize(binaryInputs, decompressedFilename)

      val gzipOutput = serializationResult.namedStreams.head.stream

      gzipOutput.writeTo(new FileOutputStream(compressedFilename))

      s"gunzip $compressedFilename" !!

      val charset = Charset.forName("UTF-8")
      val path = FileSystems.getDefault().getPath(decompressedFilename)
      var input = Files.readAllLines(path, charset)

      cleanup()

      binaryInputs map {
        case Success(e) =>
          val rawResult = input.head.getBytes

          input = input.tail

          rawResult must_== e
        case _ =>
          1 must_== 2 // Should never happen
      }
    }
  }
}
