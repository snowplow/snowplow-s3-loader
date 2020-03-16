/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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

import java.util.Properties

import cats.syntax.validated._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import org.slf4j.LoggerFactory
import com.snowplowanalytics.s3.loader.model.S3LoaderConfig
import com.snowplowanalytics.s3.loader.serializers.{GZipSerializer, LzoSerializer}
import com.snowplowanalytics.s3.loader.sinks.{KafkaSink, KinesisSink, NsqSink}

object S3Loader {

  val log = LoggerFactory.getLogger(getClass)

  def run(config: S3LoaderConfig): Unit = {
    val tracker = config.monitoring.map(e => SnowplowTracking.initializeTracker(e.snowplow))
    val maxConnectionTime = config.s3.maxTimeout

    // TODO: make the conf file more like the Elasticsearch equivalent
    val kinesisSinkRegion = config.kinesis.region
    val kinesisSinkEndpoint = config.kinesis.endpoint
    val kinesisSinkName = config.streams.outStreamName

    val credentials = CredentialsLookup.getCredentialsProvider(config.aws.accessKey, config.aws.secretKey)

    val badSink = config.sink match {
      case "kinesis" => new KinesisSink(credentials, kinesisSinkEndpoint, kinesisSinkRegion, kinesisSinkName, tracker)
      case "nsq" => new NsqSink(config)
      case "kafka" => new KafkaSink(config)
    }

    val serializer = config.s3.format match {
      case "lzo" => LzoSerializer
      case "gzip" => GZipSerializer
      case _ => throw new IllegalArgumentException("Invalid serializer. Check s3.format key in configuration file")
    }

    val executor = config.source match {
      // Read records from Kinesis
      case "kinesis" =>
        new KinesisSourceExecutor(convertConfig(config, credentials),
          config.kinesis.initialPosition,
          config.kinesis.timestamp,
          config.s3,
          badSink,
          serializer,
          maxConnectionTime,
          tracker
        ).valid
      // Read records from NSQ
      case "nsq" =>
        new NsqSourceExecutor(config,
          credentials,
          badSink,
          serializer,
          maxConnectionTime,
          tracker
        ).valid
      // Read records from Kafka
      case "kafka" =>
        new KafkaSourceExecutor(config,
          credentials,
          badSink,
          serializer,
          maxConnectionTime,
          tracker
        ).valid

      case _ => s"Source must be set to one of 'kinesis', 'NSQ' or 'kafka'. Provided: ${config.source}".invalid
    }

    executor.fold(
      err => throw new IllegalArgumentException(err),
      exec => {
        tracker foreach {
          t => SnowplowTracking.initializeSnowplowTracking(t)
        }
        exec.run()

        // If the stream cannot be found, the KCL's "cw-metrics-publisher" thread will prevent the
        // application from exiting naturally so we explicitly call System.exit.
        // This does not apply to NSQ because NSQ consumer is non-blocking and fall here
        // right after consumer.start()
        config.source match {
          case "kinesis" | "kafka" => System.exit(1)
          // do anything
          case "nsq" =>
        }
      }
    )
  }

  /**
   * This function converts the config file into the format
   * expected by the Kinesis connector interfaces.
   *
   * @param conf The configuration HOCON
   * @return A KinesisConnectorConfiguration
   */
  def convertConfig(conf: S3LoaderConfig, credentials: AWSCredentialsProvider): KinesisConnectorConfiguration = {
    val props = new Properties()

    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, conf.streams.inStreamName)
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, conf.kinesis.endpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, conf.kinesis.appName)
    props.setProperty(KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM, conf.kinesis.initialPosition)

    props.setProperty(KinesisConnectorConfiguration.PROP_S3_ENDPOINT, conf.s3.endpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_S3_BUCKET, conf.s3.bucket)

    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, conf.streams.buffer.byteLimit.toString)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT, conf.streams.buffer.recordLimit.toString)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT, conf.streams.buffer.timeLimit.toString)

    props.setProperty(KinesisConnectorConfiguration.PROP_CONNECTOR_DESTINATION, "s3")

    // So that the region of the DynamoDB table is correct
    props.setProperty(KinesisConnectorConfiguration.PROP_REGION_NAME, conf.kinesis.region)

    // The emit method retries sending to S3 indefinitely, so it only needs to be called once
    props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, "1")

    props.setProperty(KinesisConnectorConfiguration.PROP_MAX_RECORDS, conf.kinesis.maxRecords.toString)

    log.info(s"Initializing sink with KinesisConnectorConfiguration: $props")

    new KinesisConnectorConfiguration(props, credentials)
  }
}
