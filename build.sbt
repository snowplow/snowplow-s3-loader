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

lazy val root = project.in(file("."))
  .aggregate(main, distroless, lzo)

lazy val main = project.in(file("modules/main"))
  .settings(BuildSettings.mainSettings)
  .settings(
    libraryDependencies ++= Dependencies.mainDependencies,
    excludeDependencies ++= Dependencies.mainExclusions
  )
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin)

lazy val distroless = project.in(file("modules/distroless"))
  .settings(BuildSettings.distrolessSettings)
  .settings(sourceDirectory := (main / sourceDirectory).value)
  .settings(
    libraryDependencies ++= Dependencies.mainDependencies,
    excludeDependencies ++= Dependencies.mainExclusions
  )
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin)

lazy val lzo = project.in(file("modules/lzo"))
  .settings(BuildSettings.lzoSettings)
  .settings(
    libraryDependencies ++= Dependencies.lzoDependencies,
    excludeDependencies ++= Dependencies.hadoopExclusions
  )
  .dependsOn(main % "compile->compile; test->test; runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin)

shellPrompt := { _ => "s3-loader> " }
