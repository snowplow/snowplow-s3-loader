/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.blob.aws

import cats.effect.IO

import com.snowplowanalytics.snowplow.streams.kinesis.{KinesisFactory, KinesisHttpSourceConfig, KinesisSinkConfig}

import com.snowplowanalytics.snowplow.blob.core.LoaderApp

object AwsApp extends LoaderApp[EmptyConfig, KinesisHttpSourceConfig, KinesisSinkConfig](BuildInfo) {

  override def toFactory: FactoryProvider = _ => KinesisFactory.resource[IO]

  override def toBlobSink: BlobSinkProvider = _ => S3Sink.build[IO]
}
