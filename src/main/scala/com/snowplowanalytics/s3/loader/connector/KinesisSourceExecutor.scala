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
package com.snowplowanalytics.s3.loader.connector

// Logging
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleRecordsFetcherFactory
import com.snowplowanalytics.s3.loader.Config.{InitialPosition, S3}
import com.snowplowanalytics.s3.loader.serializers.ISerializer
import com.snowplowanalytics.s3.loader.{EmitterInput, KinesisSink, ValidatedRecord}
import org.slf4j.LoggerFactory

import java.time.Duration

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{KinesisConnectorConfiguration, KinesisConnectorExecutorBase, KinesisConnectorRecordProcessorFactory}

// AWS Client Library
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// cats
import cats.Id

/**
 * A worker (Runnable) class for Kinesis Connector,
 * initializes config and passes control over to [[KinesisS3Pipeline]]
 */
class KinesisSourceExecutor(config: KinesisConnectorConfiguration,
                            initialPosition: InitialPosition,
                            s3Config: S3,
                            badSink: KinesisSink,
                            serializer: ISerializer,
                            maxConnectionTime: Long,
                            tracker: Option[Tracker[Id]],
                            disableCloudWatch: Boolean
) extends KinesisConnectorExecutorBase[ValidatedRecord, EmitterInput] {

  private val logger = LoggerFactory.getLogger(getClass)

  if (disableCloudWatch)
    initialize(config, new NullMetricsFactory())
  else
    initialize(config, null)

  def getKCLConfig(initialPosition: InitialPosition, kcc: KinesisConnectorConfiguration): KinesisClientLibConfiguration = {
    val cfg = new KinesisClientLibConfiguration(kcc.APP_NAME,
      kcc.KINESIS_INPUT_STREAM,
      kcc.KINESIS_ENDPOINT,
      kcc.DYNAMODB_ENDPOINT,
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
    val pipeline = new KinesisS3Pipeline(s3Config, badSink, serializer, maxConnectionTime, tracker)
    new KinesisConnectorRecordProcessorFactory[ValidatedRecord, EmitterInput](pipeline, config)
  }
}
