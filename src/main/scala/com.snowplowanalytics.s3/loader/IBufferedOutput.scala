package com.snowplowanalytics.s3.loader

import cats.syntax.option._
import com.snowplowanalytics.s3.loader.model.S3Config
import com.snowplowanalytics.s3.loader.serializers.ISerializer
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.Logger

import scala.collection.JavaConversions._

trait IBufferedOutput {
  val log: Logger
  val serializer: ISerializer
  val s3Emitter: S3Emitter
  val s3Config: S3Config

  def flushMessages(messages: List[EmitterInput],
                    bufferStartTime: Long,
                    bufferEndTime: Long): Unit = {
    val baseFilename = getBaseFilename(bufferStartTime, bufferEndTime)
    val serializationResults =
      serializer.serialize(messages, baseFilename)
    val (successes, failures) =
      serializationResults.results.partition(_.isValid)

    log.info(
      s"Successfully serialized ${successes.size} records out of ${successes.size + failures.size}"
    )

    if (successes.nonEmpty) {
      serializationResults.namedStreams.foreach { stream =>
        val connectionAttemptStartTime = System.currentTimeMillis()
        s3Emitter.attemptEmit(stream, false, connectionAttemptStartTime) match {
          case false => log.error(s"Error while sending to S3")
          case true =>
            log.info(s"Successfully sent ${successes.size} records")
        }
      }
    }

    if (failures.nonEmpty) {
      s3Emitter.sendFailures(failures)
    }
  }

  private[this] def getBaseFilename(startTime: Long, endTime: Long): String = {
    val currentTimeObject = new DateTime(System.currentTimeMillis())
    val startTimeObject = new DateTime(startTime)
    val endTimeObject = new DateTime(endTime)

    val fileName = (s3Config.filenamePrefix ::
      DateFormat.print(currentTimeObject).some ::
      TimeFormat.print(startTimeObject).some ::
      TimeFormat.print(endTimeObject).some ::
      math.abs(util.Random.nextInt).toString.some ::
      Nil).flatten

    val baseFolder = s3Config.outputDirectory ::
      formatFolderDatePrefix(currentTimeObject) :: fileName
      .mkString("-").some ::
      Nil

    val baseName = baseFolder.flatten.mkString("/")
    baseName
  }

  private[this] val TimeFormat =
    DateTimeFormat.forPattern("HHmmssSSS").withZone(DateTimeZone.UTC)
  private[this] val DateFormat =
    DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC)

  private[this] val folderDateFormat =
    s3Config.dateFormat.map(format => DateTimeFormat.forPattern(format))
  private[this] def formatFolderDatePrefix(currentTime: DateTime): Option[String] =
    folderDateFormat.map(formatter => formatter.print(currentTime))
}
