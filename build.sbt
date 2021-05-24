/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
  .settings(
    name        := "snowplow-s3-loader",
    version     := "2.0.0",
    description := "Load the contents of a Kinesis stream topic to S3"
  )
  .settings(BuildSettings.basicSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(BuildSettings.dockerSettings)
  .settings(BuildSettings.addExampleConfToTestCp)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.Libraries.kinesisClient,
      Dependencies.Libraries.kinesisConnector,
      Dependencies.Libraries.slf4j,
      Dependencies.Libraries.log4jOverSlf4j,
      Dependencies.Libraries.log4jCore,
      Dependencies.Libraries.log4jApi,
      Dependencies.Libraries.hadoop,
      Dependencies.Libraries.elephantbird,
      Dependencies.Libraries.hadoopLZO,
      Dependencies.Libraries.apacheCommons,
      Dependencies.Libraries.jackson,
      Dependencies.Libraries.jacksonCbor,
      Dependencies.Libraries.sentry,
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
      Dependencies.Libraries.collectorPayload
    )
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin)

shellPrompt := { _ => "s3-loader> " }
