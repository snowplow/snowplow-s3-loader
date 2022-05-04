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
  .aggregate(main, lzo)

lazy val main = project.in(file("modules/main"))
  .settings(
    name := "snowplow-s3-loader",
  )
  .settings(BuildSettings.commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.Libraries.kinesisClient,
      Dependencies.Libraries.kinesisConnector,
      Dependencies.Libraries.slf4j,
      Dependencies.Libraries.jclOverSlf4j,
      Dependencies.Libraries.jackson,
      Dependencies.Libraries.sentry,
      Dependencies.Libraries.jaxbApi,
      // Scala
      Dependencies.Libraries.decline,
      Dependencies.Libraries.circe,
      Dependencies.Libraries.snowplowTracker,
      Dependencies.Libraries.snowplowBadrows,
      Dependencies.Libraries.pureconfig,
      Dependencies.Libraries.pureconfigCirce,
      // Scala (test only)
      Dependencies.Libraries.specs2,
      // Thrift (test only)
      Dependencies.Libraries.collectorPayload,
      Dependencies.Libraries.thrift % Test,
    ),
    excludeDependencies += "commons-logging" % "commons-logging"
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin)

lazy val lzo = project.in(file("modules/lzo"))
  .settings(
    name := "snowplow-s3-loader-lzo",
  )
  .settings(BuildSettings.commonSettings)
  .settings(BuildSettings.lzoSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.hadoop,
      Dependencies.Libraries.elephantbird,
      Dependencies.Libraries.hadoopLZO,
      Dependencies.Libraries.thrift,
      Dependencies.Libraries.collections,
      Dependencies.Libraries.jacksonCbor,
    )
  )
  .dependsOn(main % "compile->compile; test->test")
  .enablePlugins(JavaAppPackaging, DockerPlugin)

shellPrompt := { _ => "s3-loader> " }
