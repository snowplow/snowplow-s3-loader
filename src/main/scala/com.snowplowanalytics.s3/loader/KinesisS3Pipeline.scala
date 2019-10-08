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

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.interfaces.{IBuffer, IEmitter, IFilter, IKinesisConnectorPipeline, ITransformer}
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.impl.{AllPassFilter, BasicMemoryBuffer}

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// This project
import sinks._
import serializers._
import model._

/**
 * S3Pipeline class sets up the Emitter/Buffer/Transformer/Filter
 * Comes from Kinesis Connectors
 */
class KinesisS3Pipeline(s3Config: S3Config, badSink: ISink, serializer: ISerializer, maxConnectionTime: Long, tracker: Option[Tracker]) extends IKinesisConnectorPipeline[ValidatedRecord, EmitterInput] {

  def getEmitter(configuration: KinesisConnectorConfiguration): IEmitter[EmitterInput]  =
    new KinesisS3Emitter(s3Config, configuration.AWS_CREDENTIALS_PROVIDER, badSink, serializer, maxConnectionTime, tracker)

  def getBuffer(configuration: KinesisConnectorConfiguration): IBuffer[ValidatedRecord] =
    new BasicMemoryBuffer[ValidatedRecord](configuration)

  def getTransformer(c: KinesisConnectorConfiguration): ITransformer[ValidatedRecord, EmitterInput] =
    new RawEventTransformer()

  def getFilter(c: KinesisConnectorConfiguration): IFilter[ValidatedRecord] =
    new AllPassFilter[ValidatedRecord]()

}

