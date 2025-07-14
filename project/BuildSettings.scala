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
import Keys._

import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.archetypes.jar.LauncherJarPlugin.autoImport.packageJavaLauncherJar
import com.typesafe.sbt.packager.docker.{Cmd, DockerPermissionStrategy}
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._

// Scoverage plugin
import scoverage.ScoverageKeys._

// Scalafmt plugin
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._

// dynver plugin
import sbtdynver.DynVerPlugin.autoImport._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq(
    organization :=  "com.snowplowanalytics",
    scalaVersion :=  "2.13.16",
    description  := "Load the contents of a Kinesis stream topic to S3",
    resolvers             ++= Dependencies.resolvers,
    ThisBuild / dynverVTagPrefix := false,
    ThisBuild / dynverSeparator := "-"
  )

  /** Add example config for integration tests */
  lazy val addExampleConfToTestCp = Seq(
    Test / unmanagedClasspath += {
      baseDirectory.value / "../../config"
    }
  )

  lazy val lzoDockerSettings = Seq(
    dockerCommands := {
      val installLzo = Seq(Cmd("RUN", "mkdir -p /var/lib/apt/lists/partial && apt-get update && apt-get install -y lzop && apt-get purge -y"))
      val (h, t) = dockerCommands.value.splitAt(dockerCommands.value.size-4)
      h ++ installLzo ++ t
    },
    Docker / packageName := "snowplow-s3-loader",
    dockerAlias := dockerAlias.value.withTag(Some(version.value + "-lzo"))
  )

  // Makes our SBT app settings available from within the app
  lazy val scalifySettings = Seq(
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "settings.scala"
      IO.write(file, """package com.snowplowanalytics.s3.loader.generated
        |object Settings {
        |  val organization = "%s"
        |  val version = "%s"
        |  val name = "%s"
        |}
        |""".stripMargin.format(organization.value, version.value, name.value))
      Seq(file)
    }.taskValue
  )

  // sbt-assembly settings for building a fat jar
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(
    assembly / assemblyJarName := { s"${name.value}-${version.value}.jar" },
    assembly / assemblyMergeStrategy := {
      case PathList("javax", "servlet", xs @ _*)               => MergeStrategy.first
      case PathList("org", "objectweb", "asm", xs @ _*)        => MergeStrategy.first
      case PathList("org", "objectweb", "asm", xs @ _*)        => MergeStrategy.first
      case PathList("org", "apache", "log4j", _*)              => MergeStrategy.last 
      case PathList("org", "apache", "commons", _*)            => MergeStrategy.last
      case PathList(ps @ _*) if ps.last endsWith ".html"       => MergeStrategy.first
      case "application.conf"                                  => MergeStrategy.concat
      case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.first
      case PathList("com", "snowplowanalytics", "s3", "loader", "generated", _*) => MergeStrategy.last
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

  lazy val scoverageSettings = Seq(
    coverageMinimum := 50,
    coverageFailOnMinimum := true,
    coverageHighlighting := false,
    (Test / test) := {
      (coverageReport dependsOn (Test / test)).value
    }
  )
  lazy val formattingSettings = Seq(
    scalafmtConfig    := file(".scalafmt.conf"),
    scalafmtOnCompile := false
  )

  lazy val commonSettings = basicSettings ++ scalifySettings ++ sbtAssemblySettings ++ addExampleConfToTestCp

  lazy val mainSettings = commonSettings ++ Seq(
    name := "snowplow-s3-loader"
  )

  lazy val distrolessSettings = commonSettings ++ Seq(
    name := "snowplow-s3-loader"
  )

  lazy val lzoSettings = commonSettings ++ lzoDockerSettings ++ Seq(
    name := "snowplow-s3-loader-lzo",
    Compile / discoveredMainClasses := Seq(),
    Compile / mainClass := Some("com.snowplowanalytics.s3.loader.lzo.Main")
  )
}
