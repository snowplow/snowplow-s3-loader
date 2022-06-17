/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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
    scalaVersion :=  "2.13.6",
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

  lazy val dockerSettingsFocal = Seq(
    Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    Docker / daemonUser := "daemon",
    Docker / packageName := "snowplow/snowplow-s3-loader",
    dockerBaseImage := "eclipse-temurin:11-jre-focal",
    dockerUpdateLatest := true,
  )

  lazy val dockerSettingsDistroless = Seq(
    Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "gcr.io/distroless/java11-debian11:nonroot",
    Docker / daemonUser := "nonroot",
    Docker / daemonGroup := "nonroot",
    dockerRepository := Some("snowplow"),
    Docker / daemonUserUid := None,
    Docker / defaultLinuxInstallLocation := "/home/snowplow",
    dockerEntrypoint := Seq("java", "-jar",s"/home/snowplow/lib/${(packageJavaLauncherJar / artifactPath).value.getName}"),
    dockerPermissionStrategy := DockerPermissionStrategy.CopyChown,
    dockerAlias := dockerAlias.value.withTag(Some(version.value + "-distroless")),
    dockerUpdateLatest := false
  )

  lazy val lzoDockerSettingsFocal = dockerSettingsFocal ++ Seq(
    dockerCommands := {
      val installLzo = Seq(Cmd("RUN", "mkdir -p /var/lib/apt/lists/partial && apt-get update && apt-get install -y lzop && apt-get purge -y"))
      val (h, t) = dockerCommands.value.splitAt(dockerCommands.value.size-4)
      h ++ installLzo ++ t
    },
    dockerAlias := dockerAlias.value.withTag(Some(version.value + "-lzo")),
    dockerUpdateLatest := false
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

  lazy val mainSettings = commonSettings ++ dockerSettingsFocal ++ Seq(
    name := "snowplow-s3-loader"
  )

  lazy val distrolessSettings = commonSettings ++ dockerSettingsDistroless ++ Seq(
    name := "snowplow-s3-loader"
  )

  lazy val lzoSettings = commonSettings ++ lzoDockerSettingsFocal ++ Seq(
    name := "snowplow-s3-loader-lzo",
    Compile / discoveredMainClasses := Seq(),
    Compile / mainClass := Some("com.snowplowanalytics.s3.loader.lzo.Main")
  )
}
