/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
                        monitoring: Monitoring)
    extends IKinesisConnectorPipeline[RawRecord, Result] {

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
