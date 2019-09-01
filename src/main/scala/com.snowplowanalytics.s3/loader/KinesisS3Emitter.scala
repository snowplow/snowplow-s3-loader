/*
 * Copyright (c) 2014-2015 Snowplow Analytics Ltd. All rights reserved.
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

// Scala
import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.{Success => TrySuccess}

// Java libs
import java.util.Calendar
import java.text.SimpleDateFormat

//AWS libs
import com.amazonaws.auth.AWSCredentialsProvider

// AWS Kinesis connector libs
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

// Scala
import scala.collection.JavaConversions._

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// Json4s
import org.json4s.jackson.JsonMethods.parse

// Iglu Core
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.json4s.implicits._

// Scalaz
import scalaz._

// This project
import sinks._
import serializers._
import model._
import KinesisS3Emitter._

/**
 * Emitter for flushing Kinesis event data to S3.
 *
 * Once the buffer is full, the emit function is called.
 */
class KinesisS3Emitter(
  s3Config: S3Config,
  provider: AWSCredentialsProvider,
  badSink: ISink, 
  serializer: ISerializer, 
  maxConnectionTime: Long, 
  tracker: Option[Tracker],
  partition: Boolean,
  partitionErrorDir: String
) extends IEmitter[EmitterInput]  {

  val s3Emitter = new S3Emitter(s3Config, provider, badSink, maxConnectionTime, tracker)

  /**
   * Reads items from a buffer and saves them to s3.
   *
   * This method is expected to return a List of items that
   * failed to be written out to S3, which will be sent to
   * a Kinesis stream for bad events.
   *
   * @param buffer BasicMemoryBuffer containing EmitterInputs
   * @return list of inputs which failed transformation
   */
  override def emit(buffer: UnmodifiableBuffer[EmitterInput]): java.util.List[EmitterInput] = {

    s3Emitter.log.info(s"Flushing buffer with ${buffer.getRecords.size} records.")

    val records = buffer.getRecords().asScala.toList
    if (partition) {
      partitionWithSchemaKey(records, partitionErrorDir).foldLeft(List[EmitterInput]()) {
        case (acc, (prefix, l)) =>
          val baseFilename = getBaseFilename(buffer.getFirstSequenceNumber, buffer.getLastSequenceNumber, Some(prefix.getName))
          acc ::: emitRecords(l, baseFilename)
      }
    } else {
      val baseFilename = getBaseFilename(buffer.getFirstSequenceNumber, buffer.getLastSequenceNumber)
      emitRecords(records, baseFilename)
    }
  }

  private def emitRecords(records: List[EmitterInput], baseFilename: String) = {
    val serializationResults = serializer.serialize(records, baseFilename)
    val (successes, failures) = serializationResults.results.partition(_.isSuccess)

    s3Emitter.log.info(s"Successfully serialized ${successes.size} records out of ${successes.size + failures.size}")

    val connectionAttemptStartTime = System.currentTimeMillis()

    if (successes.size > 0) {
      serializationResults.namedStreams.foreach { s3Emitter.attemptEmit(_, connectionAttemptStartTime) }
      failures
    } else {
      failures
    }
  }

  /**
   * Closes the client when the KinesisConnectorRecordProcessor is shut down
   */
  override def shutdown(): Unit =
    s3Emitter.client.shutdown

  /**
   * Sends records which fail deserialization or compression
   * to Kinesis with an error message
   *
   * @param records List of failed records to send to Kinesis
   */
  override def fail(records: java.util.List[EmitterInput]): Unit =
    s3Emitter.sendFailures(records)
}

object KinesisS3Emitter {

  /** Type of row which determined according to schema of self describing data */
  sealed trait RowType extends Product with Serializable {
    def getName: String
  }

  object RowType {
    /** Represents cases where row type could not be determined
      * since either row is not valid json or it is not self
      * describing json
      */
    case class PartitionError(errorDir: String) extends RowType {
      override def getName: String = errorDir
    }

    /** Represents cases where type of row can be determined successfully
      * e.g. does have proper schema key
      */
    case class SelfDescribing(rowType: String) extends RowType {
      override def getName: String = rowType
    }

    case object UnexpectedError extends RowType {
      override def getName: String = "unexpected_error"
    }
  }

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  /**
    * Determines the filename in S3, which is the corresponding
    * Kinesis sequence range of records in the file.
    */
  private def getBaseFilename(firstSeq: String, lastSeq: String, prefix: Option[String] = None): String =
    prefix.map(p => if (p.isEmpty) "" else p + "/").getOrElse("") + dateFormat.format(Calendar.getInstance().getTime()) +
      "-" + firstSeq + "-" + lastSeq

  /**
    * Assume records are self describing data and group them according
    * to their schema key. Put records which are not self describing data
    * to under "old bad row type".
    */
  private[loader] def partitionWithSchemaKey(records: List[EmitterInput], errorDir: String) = {
    records.groupBy {
      case Success(byteRecord) =>
        val strRecord = new String(byteRecord, "UTF-8")
        Try(parse(strRecord)) match {
          case TrySuccess(e) =>
            val json = parse(strRecord)
            val schemaKey = SchemaKey.extract(json)
            schemaKey.fold(
              e => RowType.PartitionError(errorDir),
              k => RowType.SelfDescribing(s"${k.vendor}.${k.name}")
            )
          case _ => RowType.PartitionError(errorDir)
        }
      case _ => RowType.UnexpectedError
    }
  }
}
