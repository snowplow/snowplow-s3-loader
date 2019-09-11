package com.snowplowanalytics.s3.loader.serializers

// Java
import java.io.{
  File,
  FileOutputStream
}
import java.nio.file.{
  FileSystems,
  Files
}
import java.nio.charset.Charset

// cats
import cats.data.Validated
import cats.syntax.validated._

// Scala
import scala.sys.process._
import scala.collection.JavaConversions._

// Specs2
import org.specs2.mutable.Specification

class GZipSerializerSpec extends Specification {

  "The GZipSerializer" should {
    "correctly serialize and compress a list of CollectorPayloads" in {
      val decompressedFilename = "/tmp/kinesis-s3-sink-test-gzip"

      val compressedFilename = decompressedFilename + ".gz"

      def cleanup() = List(compressedFilename, decompressedFilename).foreach(new File(_).delete())

      cleanup()

      val binaryInputs = List(
        List("A", "B", 1000, "a", "b").mkString("\t").getBytes.valid,
        List("X", "Y", 2000, "x", "y").mkString("\t").getBytes.valid
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
        case Validated.Valid(e) =>
          val rawResult = input.head.getBytes

          input = input.tail

          rawResult must_== e
        case _ =>
          1 must_== 2 // Should never happen
      }
    }
  }
}
