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

import java.nio.ByteBuffer
import java.net.URI

import cats.implicits._

import cats.effect.{Async, Resource, Sync}

import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import com.snowplowanalytics.snowplow.blob.core.BlobSink

object S3Sink {
  def build[F[_]: Async]: Resource[F, BlobSink[F]] = mkClient.map { s3Client =>
    new BlobSink[F] {
      override def write(uri: URI, events: ByteBuffer): F[Unit] =
        Async[F].fromCompletableFuture {
          Sync[F].delay {
            val bucket     = uri.getHost
            val putRequest = PutObjectRequest.builder.bucket(bucket).key(uri.getPath.stripPrefix("/")).build
            val data       = AsyncRequestBody.fromByteBufferUnsafe(events)
            s3Client.putObject(putRequest, data)
          }
        }.void
    }
  }

  private def mkClient[F[_]: Sync]: Resource[F, S3AsyncClient] =
    for {
      httpClient <- Resource.fromAutoCloseable {
                      Sync[F].delay {
                        NettyNioAsyncHttpClient.builder.build
                      }
                    }
      s3Client <- Resource.fromAutoCloseable {
                    Sync[F].delay {
                      S3AsyncClient.builder
                        .httpClient(httpClient)
                        .defaultsMode(DefaultsMode.AUTO)
                        .build
                    }
                  }
    } yield s3Client
}
