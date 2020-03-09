package com.snowplowanalytics.s3.loader

import com.snowplowanalytics.s3.loader.model.S3Config
import com.snowplowanalytics.s3.loader.serializers.ISerializer
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.slf4j.Logger

import scala.collection.JavaConversions._

trait IBufferedOutput {
  val log: Logger
  val serializer: ISerializer
  val s3Emitter: S3Emitter
  val s3Config: S3Config

  def getBaseFilename(startTime: Long, endTime: Long): String = {
    val currentTimeObject = new DateTime(System.currentTimeMillis())
    val startTimeObject = new DateTime(startTime)
    val endTimeObject = new DateTime(endTime)

    val fileName = (s3Config.filenamePrefix :: Some(
      DateFormat.print(currentTimeObject)
    ) :: Some(TimeFormat.print(startTimeObject)) :: Some(
      TimeFormat.print(endTimeObject)
    ) :: Some(math.abs(util.Random.nextInt).toString) :: Nil).flatten

    val baseFolder = s3Config.outputDirectory :: formatFolderDatePrefix(
      currentTimeObject
    ) :: Some(fileName.mkString("-")) :: Nil

    val baseName = baseFolder.flatten.mkString("/")
    baseName
  }

  private val TimeFormat =
    DateTimeFormat.forPattern("HHmmssSSS").withZone(DateTimeZone.UTC)
  private val DateFormat =
    DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC)

  private val folderDateFormat =
    s3Config.dateFormat.map(format => DateTimeFormat.forPattern(format))
  private def formatFolderDatePrefix(currentTime: DateTime): Option[String] =
    folderDateFormat.map(formatter => formatter.print(currentTime))

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
}
