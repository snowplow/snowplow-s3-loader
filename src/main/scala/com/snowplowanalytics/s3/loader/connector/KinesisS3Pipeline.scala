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

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.impl.{AllPassFilter, BasicMemoryBuffer}
import com.amazonaws.services.kinesis.connectors.interfaces._

import com.snowplowanalytics.s3.loader.{KinesisSink, RawRecord, Result}
import com.snowplowanalytics.s3.loader.Config.{Output, Purpose}
import com.snowplowanalytics.s3.loader.monitoring.Monitoring
import com.snowplowanalytics.s3.loader.serializers.ISerializer

/**
 * S3Pipeline class sets up the Emitter/Buffer/Transformer/Filter
 * Comes from Kinesis Connectors
 */
class KinesisS3Pipeline(client: AmazonS3,
                        purpose: Purpose,
                        output: Output,
                        badSink: KinesisSink,
                        serializer: ISerializer,
                        monitoring: Monitoring
) extends IKinesisConnectorPipeline[RawRecord, Result] {

  def getEmitter(c: KinesisConnectorConfiguration): IEmitter[Result] =
    new KinesisS3Emitter(client, monitoring, purpose, output, badSink, serializer)

  def getBuffer(c: KinesisConnectorConfiguration): IBuffer[RawRecord] =
    new BasicMemoryBuffer[RawRecord](c)

  def getTransformer(
    c: KinesisConnectorConfiguration
  ): ITransformer[RawRecord, Result] =
    new IdentityTransformer()

  def getFilter(c: KinesisConnectorConfiguration): IFilter[RawRecord] =
    new AllPassFilter[RawRecord]()
}

object KinesisS3Pipeline {
  def buildS3Client(region: Option[String], customEndpoint: Option[String]): AmazonS3 = {
    val client = AmazonS3ClientBuilder.standard()
    customEndpoint match {
      case Some(value) =>
        client.setEndpointConfiguration(
          new EndpointConfiguration(value, region.orNull)
        )
      case None => ()
    }
    client.build()
  }
}
