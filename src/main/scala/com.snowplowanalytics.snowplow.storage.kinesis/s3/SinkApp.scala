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
package com.snowplowanalytics.snowplow.storage.kinesis.s3

// Java
import java.io.File
import java.util.Properties

// Argot
import org.clapper.argot._

// Config
import com.typesafe.config.{Config, ConfigFactory}

// AWS libs
import com.amazonaws.auth.AWSCredentialsProvider

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

// Loggings
import org.apache.commons.logging.LogFactory

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// Scalaz
import scalaz._
import Scalaz._

// This project
import sinks._
import serializers._

/**
 * The entrypoint class for the Kinesis-S3 Sink applciation.
 */
object SinkApp extends App {

  val log = LogFactory.getLog(getClass)

  // Argument specifications
  import ArgotConverters._

  // General bumf for our app
  val parser = new ArgotParser(
    programName = "generated",
    compactUsage = true,
    preUsage = Some("%s: Version %s. Copyright (c) 2014-2015, %s.".format(
      generated.Settings.name,
      generated.Settings.version,
      generated.Settings.organization)
    )
  )

  // Optional config argument
  val config = parser.option[Config](List("config"),
                                     "filename",
                                     "Configuration file.") {
    (c, opt) =>

      val file = new File(c)
      if (file.exists) {
        ConfigFactory.parseFile(file)
      } else {
        parser.usage("Configuration file \"%s\" does not exist".format(c))
        ConfigFactory.empty()
      }
  }

  // Optional in-stream name
  val streamName = parser.option[String](List("stream-name"),
                                 "stream name",
                                 "Kinesis In-Stream Name")

  parser.parse(args)

  val conf = config.value.getOrElse(throw new RuntimeException("--config argument must be provided"))
  // Default to the stream specified in the config
  val inStream = streamName.value.getOrElse(
    conf.getConfig("sink").getConfig("kinesis").getConfig("in").getString("stream-name"))

  val tracker = if (conf.hasPath("sink.monitoring.snowplow")) {
    SnowplowTracking.initializeTracker(conf.getConfig("sink.monitoring.snowplow")).some
  } else {
    None
  }

  val maxConnectionTime = conf.getConfig("sink").getConfig("s3").getLong("max-timeout")

  // TODO: make the conf file more like the Elasticsearch equivalent
  val kinesisSinkRegion = conf.getConfig("sink").getConfig("kinesis").getString("region")
  val kinesisSinkEndpoint = s"https://kinesis.${kinesisSinkRegion}.amazonaws.com"
  val kinesisSink = conf.getConfig("sink").getConfig("kinesis").getConfig("out")

  // Append _failed to the inStream. Failed messages will be written to this stream.
  val kinesisSinkName = s"${inStream}_failed"

  val logLevel = conf.getConfig("sink").getConfig("logging").getString("level")
  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel)

  val credentialConfig = conf.getConfig("sink").getConfig("aws")

  val credentials = CredentialsLookup.getCredentialsProvider(credentialConfig.getString("access-key"), credentialConfig.getString("secret-key"))

  val badSink = new KinesisSink(credentials, kinesisSinkEndpoint, kinesisSinkName, tracker)

  val serializer = conf.getConfig("sink").getConfig("s3").getString("format") match {
    case "lzo" => LzoSerializer
    case "gzip" => GZipSerializer
    case _ => throw new Exception("Invalid serializer. Check sink.s3.format key in configuration file")
  }

  val executor = new S3SinkExecutor(convertConfig(conf, credentials, inStream), badSink, serializer, maxConnectionTime, tracker)

  tracker match {
    case Some(t) => SnowplowTracking.initializeSnowplowTracking(t)
    case None => None
  }

  executor.run()

  /**
   * This function converts the config file into the format
   * expected by the Kinesis connector interfaces.
   *
   * @param conf The configuration HOCON
   * @param credentials AWS Credentials
   * @param inStream
   * @return A KinesisConnectorConfiguration
   */
  def convertConfig(conf: Config, credentials: AWSCredentialsProvider, inStream: String): KinesisConnectorConfiguration = {
    val props = new Properties()
    val connector = conf.resolve.getConfig("sink")

    val kinesis = connector.getConfig("kinesis")
    val kinesisIn = kinesis.getConfig("in")
    val kinesisRegion = kinesis.getString("region")
    val kEndpoint = s"https://kinesis.${kinesisSinkRegion}.amazonaws.com"
    val streamName = inStream

    val initialPosition = kinesisIn.getString("initial-position")

    // KCL creates a Dynamo table with the appName to create the checkpoints
    val appName =  s"kinesis_s3_$streamName"

    val s3 = connector.getConfig("s3")
    val s3Region = s3.getString("region")
    val s3Endpoint = s3Region match {
      case "us-east-1" => "https://s3.amazonaws.com"
      case _ => s"https://s3-$s3Region.amazonaws.com"
    }
    val bucket = s3.getString("bucket")

    val buffer = connector.getConfig("buffer")
    val byteLimit = buffer.getString("byte-limit")
    val recordLimit = buffer.getString("record-limit")
    val timeLimit = buffer.getString("time-limit")

    val maxRecords = kinesisIn.getString("max-records")

    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, streamName)
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, kEndpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, appName)
    props.setProperty(KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM, initialPosition)

    props.setProperty(KinesisConnectorConfiguration.PROP_S3_ENDPOINT, s3Endpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_S3_BUCKET, bucket)

    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, byteLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT, recordLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT, timeLimit)

    props.setProperty(KinesisConnectorConfiguration.PROP_CONNECTOR_DESTINATION, "s3")

    // So that the region of the DynamoDB table is correct
    props.setProperty(KinesisConnectorConfiguration.PROP_REGION_NAME, kinesisRegion)

    // The emit method retries sending to S3 indefinitely, so it only needs to be called once
    props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, "1")

    props.setProperty(KinesisConnectorConfiguration.PROP_MAX_RECORDS, maxRecords)

    log.info(s"Initializing sink with KinesisConnectorConfiguration: $props")

    new KinesisConnectorConfiguration(props, credentials)
  }

}
