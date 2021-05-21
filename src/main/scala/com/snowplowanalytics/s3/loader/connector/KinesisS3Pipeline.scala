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

// AWS Kinesis Connector libs
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.impl.{AllPassFilter, BasicMemoryBuffer}
import com.amazonaws.services.kinesis.connectors.interfaces._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import com.snowplowanalytics.s3.loader.Config.{Output, Purpose}
import com.snowplowanalytics.s3.loader.serializers.ISerializer
import com.snowplowanalytics.s3.loader.{EmitterInput, KinesisSink, ValidatedRecord}

// cats
import cats.Id

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

/**
 * S3Pipeline class sets up the Emitter/Buffer/Transformer/Filter
 * Comes from Kinesis Connectors
 */
class KinesisS3Pipeline(client: AmazonS3,
                        purpose: Purpose,
                        output: Output,
                        badSink: KinesisSink,
                        serializer: ISerializer,
                        tracker: Option[Tracker[Id]]
) extends IKinesisConnectorPipeline[ValidatedRecord, EmitterInput] {

  def getEmitter(c: KinesisConnectorConfiguration): IEmitter[EmitterInput] =
    new KinesisS3Emitter(client, tracker, purpose, output, badSink, serializer)

  def getBuffer(c: KinesisConnectorConfiguration): IBuffer[ValidatedRecord] =
    new BasicMemoryBuffer[ValidatedRecord](c)

  def getTransformer(c: KinesisConnectorConfiguration): ITransformer[ValidatedRecord, EmitterInput] =
    new IdentityTransformer()

  def getFilter(c: KinesisConnectorConfiguration): IFilter[ValidatedRecord] =
    new AllPassFilter[ValidatedRecord]()

}

object KinesisS3Pipeline {
  def buildS3Client(region: Option[String], customEndpoint: Option[String]): AmazonS3 = {
    val client = AmazonS3ClientBuilder.standard()
    customEndpoint match {
      case Some(value) => client.setEndpointConfiguration(new EndpointConfiguration(value, region.orNull))
      case None => ()
    }
    client.build()
  }
}