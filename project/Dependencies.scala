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
import sbt._

object Dependencies {

  object V {
    // Java
    val commonsLang = "3.19.0"
    val jackson     = "2.19.0"
    val sentry      = "8.16.0"
    val slf4j       = "2.0.17"
    val awsSdk      = "2.33.11"

    // Scala
    val betterMonadicFor = "0.3.1"
    val circe            = "0.14.4"
    val decline          = "2.5.0"
    val http4s           = "0.23.31"

    // Snowplow
    val commonStreams = "0.14.1"
    val badRows       = "2.3.0"

    // Tests
    val analyticsSdk = "3.2.2"
    val catsEffect   = "3.6.1"
    val specs2       = "4.21.0"
    val specs2CE     = "1.6.0"
  }

  // Java
  val commonsLang = "org.apache.commons"         % "commons-lang3" % V.commonsLang
  val jackson     = "com.fasterxml.jackson.core" % "jackson-core"  % V.jackson
  val sentry      = "io.sentry"                  % "sentry"        % V.sentry
  val s3          = "software.amazon.awssdk"     % "s3"            % V.awsSdk
  val sts         = "software.amazon.awssdk"     % "sts"           % V.awsSdk % Runtime
  val slf4j       = "org.slf4j"                  % "slf4j-simple"  % V.slf4j

  // Scala
  val betterMonadicFor   = "com.olegpy"   %% "better-monadic-for"   % V.betterMonadicFor
  val circeGenericExtras = "io.circe"     %% "circe-generic-extras" % V.circe
  val decline            = "com.monovore" %% "decline-effect"       % V.decline
  val emberServer        = "org.http4s"   %% "http4s-ember-server"  % V.http4s

  // Snowplow
  val badRows         = "com.snowplowanalytics" %% "snowplow-badrows" % V.badRows
  val kinesisSnowplow = "com.snowplowanalytics" %% "kinesis"          % V.commonStreams
  val runtime         = "com.snowplowanalytics" %% "runtime-common"   % V.commonStreams
  val streams         = "com.snowplowanalytics" %% "streams-core"     % V.commonStreams

  // Tests
  val analyticsSdk      = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk % Test
  val catsEffectTestkit = "org.typelevel"         %% "cats-effect-testkit"          % V.catsEffect   % Test
  val specs2            = "org.specs2"            %% "specs2-core"                  % V.specs2       % Test
  val specs2CE          = "org.typelevel"         %% "cats-effect-testing-specs2"   % V.specs2CE     % Test

  val coreDependencies = Seq(
    // Java
    commonsLang, // for security vulnerabilities
    jackson, // for security vulnerabilities
    sentry,
    slf4j,
    // Scala
    circeGenericExtras,
    decline,
    emberServer, // for security vulnerabilities
    // Snowplow
    badRows,
    runtime,
    streams,
    // Tests
    analyticsSdk,
    catsEffectTestkit,
    specs2,
    specs2CE
  )

  val awsDependencies = Seq(
    kinesisSnowplow,
    s3,
    sts,
    // Tests
    specs2,
    specs2CE
  )
}
