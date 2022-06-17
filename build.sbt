/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */

lazy val root = project.in(file("."))
  .aggregate(main, distroless, lzo)

lazy val main = project.in(file("modules/main"))
  .settings(BuildSettings.mainSettings)
  .settings(
    libraryDependencies ++= Dependencies.mainDependencies,
    excludeDependencies ++= Dependencies.mainExclusions
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin)

lazy val distroless = project.in(file("modules/distroless"))
  .settings(BuildSettings.distrolessSettings)
  .settings(sourceDirectory := (main / sourceDirectory).value)
  .settings(
    libraryDependencies ++= Dependencies.mainDependencies,
    excludeDependencies ++= Dependencies.mainExclusions
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin, LauncherJarPlugin)

lazy val lzo = project.in(file("modules/lzo"))
  .settings(BuildSettings.lzoSettings)
  .settings(
    libraryDependencies ++= Dependencies.lzoDependencies,
    excludeDependencies ++= Dependencies.hadoopExclusions
  )
  .dependsOn(main % "compile->compile; test->test")
  .enablePlugins(JavaAppPackaging, DockerPlugin)

shellPrompt := { _ => "s3-loader> " }
