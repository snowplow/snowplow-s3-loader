/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

import org.slf4j.LoggerFactory

import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.s3.loader.Config.Compression
import com.snowplowanalytics.s3.loader.connector.KinesisSourceExecutor
import com.snowplowanalytics.s3.loader.monitoring.Monitoring
import com.snowplowanalytics.s3.loader.serializers.{GZipSerializer, LzoSerializer}

object S3Loader {

  val logger = LoggerFactory.getLogger(getClass)

  val processor = Processor(generated.Settings.name, generated.Settings.version)

  def run(config: Config): Unit = {
    val monitoring = Monitoring.build(config.monitoring)

    // A sink for records that could not be emitted to S3
    val badSink = KinesisSink.build(config, monitoring)

    val serializer = config.output.s3.compression match {
      case Compression.Lzo  => LzoSerializer
      case Compression.Gzip => GZipSerializer
    }

    val executor =
      new KinesisSourceExecutor(
        config.region,
        toKinesisConnectorConfig(config),
        config.input.position,
        config.purpose,
        config.output,
        badSink,
        serializer,
        monitoring,
        config.monitoring
          .flatMap(_.metrics.flatMap(_.cloudWatch))
          .getOrElse(false)
      )

    monitoring.initTracking()

    try executor.run()
    catch {
      case e: Throwable =>
        monitoring.captureError(e)
        throw e
    }
  }

  /**
   * This function converts the config file into the format
   * expected by the Kinesis connector interfaces.
   *
   * @param conf The configuration HOCON
   * @return A KinesisConnectorConfiguration
   */
  def toKinesisConnectorConfig(conf: Config): KinesisConnectorConfiguration = {
    val credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance()
    val props = new Properties()

    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, conf.input.streamName)
    conf.input.customEndpoint.foreach(
      props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, _)
    )

    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, conf.input.appName)
    props.setProperty(
      KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM,
      conf.input.position.toKCL.toString
    )

    conf.output.s3.customEndpoint.foreach(
      props.setProperty(KinesisConnectorConfiguration.PROP_S3_ENDPOINT, _)
    )
    props.setProperty(KinesisConnectorConfiguration.PROP_S3_BUCKET, conf.output.s3.bucketName)

    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, conf.buffer.byteLimit.toString)
    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT,
      conf.buffer.recordLimit.toString
    )
    props.setProperty(
      KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT,
      conf.buffer.timeLimit.toString
    )

    props.setProperty(KinesisConnectorConfiguration.PROP_CONNECTOR_DESTINATION, "s3")

    // So that the region of the DynamoDB table is correct
    conf.region.foreach(
      props.setProperty(KinesisConnectorConfiguration.PROP_REGION_NAME, _)
    )

    // The emit method retries sending to S3 indefinitely, so it only needs to be called once
    props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, "1")

    props.setProperty(KinesisConnectorConfiguration.PROP_MAX_RECORDS, conf.input.maxRecords.toString)

    logger.info(s"Initializing sink with KinesisConnectorConfiguration: $props")

    new KinesisConnectorConfiguration(props, credentialsProvider)
  }
}
