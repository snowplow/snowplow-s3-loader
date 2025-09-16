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

// SBT
import sbt._
import sbt.io.IO
import Keys._

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtdynver.DynVerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._

object BuildSettings {

  lazy val commonSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.13.16",
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false,
    addCompilerPlugin(Dependencies.betterMonadicFor),
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-", // to be compatible with docker
    Compile / resourceGenerators += Def.task {
      val license = (Compile / resourceManaged).value / "META-INF" / "LICENSE"
      IO.copyFile(file("LICENSE.md"), license)
      Seq(license)
    }.taskValue,
    licenses += ("Snowplow Limited Use License Agreement", url("https://docs.snowplow.io/limited-use-license-1.1")),
    headerLicense := Some(
      HeaderLicense.Custom(
        """|Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
          |
          |This software is made available by Snowplow Analytics, Ltd.,
          |under the terms of the Snowplow Limited Use License Agreement, Version 1.1
          |located at https://docs.snowplow.io/limited-use-license-1.1
          |BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
          |OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
          |""".stripMargin
      )
    ),
    headerMappings := headerMappings.value + (HeaderFileType.conf -> HeaderCommentStyle.hashLineComment),
    Test / unmanagedClasspath += {
      baseDirectory.value / "../../config"
    }
  )

  lazy val appSettings = commonSettings ++ Seq(
    buildInfoKeys := Seq[BuildInfoKey](name, version, dockerAlias),
    buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.runtime.AppInfo")
  )

  lazy val awsSettings = appSettings ++ Seq(
    name := "snowplow-s3-loader",
    description := "Write the records of a Kinesis stream to S3",
    buildInfoPackage := "com.snowplowanalytics.snowplow.blob.aws",
    buildInfoKeys += BuildInfoKey("cloud" -> "AWS")
  ) ++ Seq(
    // used in configuration parsing unit tests
    Test / envVars := Map(
      "HOSTNAME" -> "testWorkerId"
    )
  )
}
