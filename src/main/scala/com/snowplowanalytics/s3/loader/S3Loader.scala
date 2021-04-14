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

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.metrics.RequestMetricCollector
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

import org.slf4j.LoggerFactory

import com.snowplowanalytics.s3.loader.Config.{Format, Sink, Source}
import com.snowplowanalytics.s3.loader.connector.KinesisSourceExecutor
import com.snowplowanalytics.s3.loader.serializers.{GZipSerializer, LzoSerializer}

object S3Loader {

  val logger = LoggerFactory.getLogger(getClass)

  def run(config: Config): Unit = {
    val tracker = config.monitoring.map(e => SnowplowTracking.initializeTracker(e.snowplow))

    val provider = CredentialsLookup.getCredentialsProvider(config.aws.accessKey, config.aws.secretKey)

    // A sink for records that could not be emitted to S3
    val badSink = config.sink match {
      case Sink.Kinesis =>
        val endpoint = new EndpointConfiguration(config.kinesis.endpoint, config.kinesis.region)
        val client =
          if (config.kinesis.disableCW)
            AmazonKinesisClientBuilder
              .standard()
              .withCredentials(provider)
              .withEndpointConfiguration(endpoint)
              .withMetricsCollector(RequestMetricCollector.NONE)
              .build()
          else
            AmazonKinesisClientBuilder
              .standard()
              .withCredentials(provider)
              .withEndpointConfiguration(endpoint)
              .build()

        new KinesisSink(client, config.streams.outStreamName)
    }

    val serializer = config.s3.format match {
      case Format.Lzo => LzoSerializer
      case Format.Gzip => GZipSerializer
    }

    val executor = config.source match {
      case Source.Kinesis =>
        new KinesisSourceExecutor(convertConfig(config, provider),
          config.kinesis.initialPosition,
          config.s3,
          badSink,
          serializer,
          config.s3.maxTimeout,
          tracker,
          config.kinesis.disableCW
        )
    }

    tracker.foreach { t =>
      SnowplowTracking.initializeSnowplowTracking(t)
    }
    executor.run()
  }

  /**
   * This function converts the config file into the format
   * expected by the Kinesis connector interfaces.
   *
   * @param conf The configuration HOCON
   * @return A KinesisConnectorConfiguration
   */
  def convertConfig(conf: Config, credentials: AWSCredentialsProvider): KinesisConnectorConfiguration = {
    val props = new Properties()

    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, conf.streams.inStreamName)
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, conf.kinesis.endpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, conf.kinesis.appName)
    props.setProperty(KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM, conf.kinesis.initialPosition.toKCL.toString)

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

    logger.info(s"Initializing sink with KinesisConnectorConfiguration: $props")

    new KinesisConnectorConfiguration(props, credentials)
  }
}
