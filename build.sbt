/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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
    version     := "0.7.0",
    description := "Load the contents of a Kinesis stream, NSQ or Kafka topic to S3"
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(BuildSettings.dockerSettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.Libraries.kinesisClient,
      Dependencies.Libraries.kinesisConnector,
      Dependencies.Libraries.slf4j,
      Dependencies.Libraries.hadoop,
      Dependencies.Libraries.elephantbird,
      Dependencies.Libraries.hadoopLZO,
      Dependencies.Libraries.jodaTime,
      Dependencies.Libraries.nsqClient,
      Dependencies.Libraries.jacksonCbor,
      // Scala
      Dependencies.Libraries.scopt,
      Dependencies.Libraries.config,
      Dependencies.Libraries.cats,
      Dependencies.Libraries.json4sJackson,
      Dependencies.Libraries.snowplowTracker,
      Dependencies.Libraries.pureconfig,
      Dependencies.Libraries.igluCoreJson4s,
      Dependencies.Libraries.kafka,
      // Scala (test only)
      Dependencies.Libraries.specs2,
      // Thrift (test only)
      Dependencies.Libraries.collectorPayload
    )
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin)

shellPrompt := { _ => "s3-loader> " }
