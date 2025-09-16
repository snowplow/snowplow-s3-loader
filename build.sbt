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

lazy val root = project
  .in(file("."))
  .aggregate(
    core,
    aws,
    awsDistroless
  )

lazy val core: Project = project
  .in(file("modules/core"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)

lazy val aws: Project = project
  .in(file("modules/aws"))
  .settings(BuildSettings.awsSettings)
  .settings(libraryDependencies ++= Dependencies.awsDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val awsDistroless: Project = project
  .in(file("modules/distroless/aws"))
  .settings(BuildSettings.awsSettings)
  .settings(libraryDependencies ++= Dependencies.awsDependencies)
  .settings(sourceDirectory := (aws / sourceDirectory).value)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

ThisBuild / fork := true
