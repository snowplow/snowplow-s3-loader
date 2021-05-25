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
package com.snowplowanalytics.s3.loader.connector

// Logging
import com.snowplowanalytics.s3.loader.RawRecord
import org.slf4j.LoggerFactory

import java.time.Duration

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{KinesisConnectorConfiguration, KinesisConnectorExecutorBase, KinesisConnectorRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleRecordsFetcherFactory

// AWS Client Library
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory

import com.snowplowanalytics.s3.loader.{Result, KinesisSink}
import com.snowplowanalytics.s3.loader.monitoring.Monitoring
import com.snowplowanalytics.s3.loader.Config.{InitialPosition, Output, Purpose}
import com.snowplowanalytics.s3.loader.serializers.ISerializer

/**
 * A worker (Runnable) class for Kinesis Connector,
 * initializes config and passes control over to [[KinesisS3Pipeline]]
 */
class KinesisSourceExecutor(region: Option[String],
                            config: KinesisConnectorConfiguration,
                            initialPosition: InitialPosition,
                            purpose: Purpose,
                            output: Output,
                            badSink: KinesisSink,
                            serializer: ISerializer,
                            monitoring: Monitoring,
                            enableCloudWatch: Boolean
) extends KinesisConnectorExecutorBase[RawRecord, Result] {

  private val logger = LoggerFactory.getLogger(getClass)

  initialize(config, if (enableCloudWatch) null else new NullMetricsFactory())

  def getKCLConfig(initialPosition: InitialPosition, kcc: KinesisConnectorConfiguration): KinesisClientLibConfiguration = {
    val cfg = new KinesisClientLibConfiguration(kcc.APP_NAME,
      kcc.KINESIS_INPUT_STREAM,
      kcc.KINESIS_ENDPOINT,
      null,
      kcc.INITIAL_POSITION_IN_STREAM,
      kcc.AWS_CREDENTIALS_PROVIDER,
      kcc.AWS_CREDENTIALS_PROVIDER,
      kcc.AWS_CREDENTIALS_PROVIDER,
      kcc.FAILOVER_TIME,
      kcc.WORKER_ID,
      kcc.MAX_RECORDS,
      kcc.IDLE_TIME_BETWEEN_READS,
      kcc.CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST,
      kcc.PARENT_SHARD_POLL_INTERVAL,
      kcc.SHARD_SYNC_INTERVAL,
      kcc.CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY,
      new ClientConfiguration,
      new ClientConfiguration,
      new ClientConfiguration,
      KinesisClientLibConfiguration.DEFAULT_TASK_BACKOFF_TIME_MILLIS,  // taskBackoffTimeMillis
      KinesisClientLibConfiguration.DEFAULT_METRICS_BUFFER_TIME_MILLIS, // metricsBufferTimeMillis
      KinesisClientLibConfiguration.DEFAULT_METRICS_MAX_QUEUE_SIZE, // metricsMaxQueueSize
      KinesisClientLibConfiguration.DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING, // validateSequenceNumberBeforeCheckpointing
      kcc.REGION_NAME,
      KinesisClientLibConfiguration.DEFAULT_SHUTDOWN_GRACE_MILLIS, // shutdownGraceMillis
      KinesisClientLibConfiguration.DEFAULT_DDB_BILLING_MODE, // billingMode
      new SimpleRecordsFetcherFactory, // recordsFetcherFactory
      Duration.ofMinutes(1).toMillis,
      Duration.ofMinutes(5).toMillis,
      Duration.ofMinutes(30).toMillis
    ).withUserAgent(
      kcc.APP_NAME + ","
        + kcc.CONNECTOR_DESTINATION + ","
        + KinesisConnectorConfiguration.KINESIS_CONNECTOR_USER_AGENT
    ).withCallProcessRecordsEvenForEmptyRecordList(KinesisConnectorConfiguration.DEFAULT_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST)

    initialPosition match {
      case InitialPosition.AtTimestamp(tstamp) =>
        cfg.withTimestampAtInitialPositionInStream(tstamp)
      case InitialPosition.TrimHorizon | InitialPosition.Latest  =>
        cfg.withInitialPositionInStream(initialPosition.toKCL)
    }
  }

  /**
   * Initialize the Amazon Kinesis Client Library configuration and worker with metrics factory
   *
   * @param kinesisConnectorConfiguration Amazon Kinesis connector configuration
   * @param metricFactory would be used to emit metrics in Amazon Kinesis Client Library
   */
  override def initialize(kinesisConnectorConfiguration: KinesisConnectorConfiguration, metricFactory: IMetricsFactory): Unit = {
    val kinesisClientLibConfiguration = getKCLConfig(initialPosition, kinesisConnectorConfiguration)

    if (!kinesisConnectorConfiguration.CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST)
      logger.warn(
        "The false value of callProcessRecordsEvenForEmptyList will be ignored. It must be set to true for the bufferTimeMillisecondsLimit to work correctly."
      )

    if (kinesisConnectorConfiguration.IDLE_TIME_BETWEEN_READS > kinesisConnectorConfiguration.BUFFER_MILLISECONDS_LIMIT)
      logger.warn(
        "idleTimeBetweenReads is greater than bufferTimeMillisecondsLimit. For best results, ensure that bufferTimeMillisecondsLimit is more than or equal to idleTimeBetweenReads "
      )

    // If a metrics factory was specified, use it.
    worker = {
      val init = (new Worker.Builder)
        .recordProcessorFactory(getKinesisConnectorRecordProcessorFactory())
        .config(kinesisClientLibConfiguration)
      val builder = if (metricFactory != null) init.metricsFactory(metricFactory) else init
      builder.build()
    }
    logger.info(getClass.getSimpleName + " worker created")
  }

  def getKinesisConnectorRecordProcessorFactory = {
    val client = KinesisS3Pipeline.buildS3Client(region, output.s3.customEndpoint)
    val pipeline = new KinesisS3Pipeline(client, purpose, output, badSink, serializer, monitoring)
    new KinesisConnectorRecordProcessorFactory[RawRecord, Result](pipeline, config)
  }
}
