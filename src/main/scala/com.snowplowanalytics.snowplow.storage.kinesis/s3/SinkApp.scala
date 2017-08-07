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

// Config
import com.typesafe.config.{Config, ConfigFactory}

// AWS libs
import com.amazonaws.auth.AWSCredentialsProvider

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

// SLF4j
import org.slf4j.LoggerFactory

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

  case class FileConfig(config: File = new File("."))
  val parser = new scopt.OptionParser[FileConfig](generated.Settings.name) {
    head(generated.Settings.name, generated.Settings.version)
    opt[File]("config").required().valueName("<filename>")
      .action((f: File, c: FileConfig) => c.copy(f))
      .validate(f =>
        if (f.exists) success
        else failure(s"Configuration file $f does not exist")
      )
  }

  val conf = parser.parse(args, FileConfig()) match {
    case Some(c) => ConfigFactory.parseFile(c.config).resolve()
    case None    => ConfigFactory.empty()
  }

  if (conf.isEmpty()) {
    System.err.println("Empty configuration file")
    System.exit(1)
  }

  val tracker = if (conf.hasPath("monitoring.snowplow")) {
    SnowplowTracking.initializeTracker(conf.getConfig("sink.monitoring.snowplow")).some
  } else {
    None
  }

  val maxConnectionTime = conf.getConfig("s3").getLong("max-timeout")

  // TODO: make the conf file more like the Elasticsearch equivalent
  val kinesisSinkRegion = conf.getConfig("kinesis").getString("region")
  val kinesisSinkEndpoint = getKinesisEndpoint(kinesisSinkRegion)
  val kinesisSinkName = conf.getConfig("streams").getString("stream-name-out")
  val nsqConfig = new S3LoaderNsqConfig(conf)
 
  val logLevel = conf.getConfig("logging").getString("level")
  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel)
  val log = LoggerFactory.getLogger(getClass)

  val credentialConfig = conf.getConfig("aws")

  val credentials = CredentialsLookup.getCredentialsProvider(credentialConfig.getString("access-key"), credentialConfig.getString("secret-key"))

  val badSink = conf.getString("sink") match {
    case "kinesis" => new KinesisSink(credentials, kinesisSinkEndpoint, kinesisSinkRegion, kinesisSinkName, tracker)
    case "NSQ" => new NsqSink(nsqConfig)
  }

  val serializer = conf.getConfig("s3").getString("format") match {
    case "lzo" => LzoSerializer
    case "gzip" => GZipSerializer
    case _ => throw new Exception("Invalid serializer. Check sink.s3.format key in configuration file")
  }

  val executor = conf.getString("source") match {
      // Read records from Kinesis
      case "kinesis" => new KinesisSourceExecutor(convertConfig(conf, credentials), badSink, serializer, maxConnectionTime, tracker).success
      // Read records from NSQ
      case "NSQ" => new NsqSourceExecutor(convertConfig(conf, credentials), nsqConfig, badSink, serializer, maxConnectionTime, tracker).success
      case _ => "Source must be set to kinesis' or 'NSQ'".failure
  }


  executor.fold(
    err => throw new RuntimeException(err),
    exec => {
      tracker foreach {
        t => SnowplowTracking.initializeSnowplowTracking(t)
      }
      exec.run()

      // If the stream cannot be found, the KCL's "cw-metrics-publisher" thread will prevent the
      // application from exiting naturally so we explicitly call System.exit.
      // This did not applied for NSQ because NSQ consumer is non-blocking and fall here 
      // right after consumer.start()
      conf.getString("source") match { 
        case "kinesis" => System.exit(1)
        // do anything
        case "NSQ" =>    
      }
    }
  )

  /**
   * This function converts the config file into the format
   * expected by the Kinesis connector interfaces.
   *
   * @param conf The configuration HOCON
   * @return A KinesisConnectorConfiguration
   */
  def convertConfig(conf: Config, credentials: AWSCredentialsProvider): KinesisConnectorConfiguration = {
    val props = new Properties()

    val streams = conf.getConfig("streams")
    val streamName = streams.getString("stream-name-in")
    
    val buffer = streams.getConfig("buffer")
    val byteLimit = buffer.getString("byte-limit")
    val recordLimit = buffer.getString("record-limit")
    val timeLimit = buffer.getString("time-limit")

    val kinesis = conf.getConfig("kinesis")
    val kinesisRegion = kinesis.getString("region")
    val kEndpoint = getKinesisEndpoint(kinesisRegion)
    val initialPosition = kinesis.getString("initial-position")
    val appName = kinesis.getString("app-name")

    val s3 = conf.getConfig("s3")
    val s3Region = s3.getString("region")
    val s3Endpoint = s3Region match {
      case "us-east-1" => "https://s3.amazonaws.com"
      case "cn-north-1" => "https://s3.cn-north-1.amazonaws.com.cn"
      case _ => s"https://s3-$s3Region.amazonaws.com"
    }
    val bucket = s3.getString("bucket")

    val maxRecords = kinesis.getString("max-records")

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

  private def getKinesisEndpoint(region: String): String =
    region match {
      case "cn-north-1" => "kinesis.cn-north-1.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    }
}

/**
  * Rigidly load the configuration of the NSQ here to error
  */
class S3LoaderNsqConfig(config: Config) {   
  private val nsq = config.getConfig("NSQ")
  val nsqSourceChannelName = nsq.getString("channel-name")
  val nsqHost = nsq.getString("host")
  val nsqPort = nsq.getInt("port")
  val nsqlookupPort = nsq.getInt("lookup-port")
  val nsqBufferSize = config.getInt("streams.buffer.record-limit")

  private val streams = config.getConfig("streams")
  val nsqSourceTopicName = streams.getString("stream-name-in")
  val nsqSinkTopicName = streams.getString("stream-name-out")
}