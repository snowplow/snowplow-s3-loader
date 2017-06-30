/*
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd. All rights reserved.
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
    name        := "snowplow-kinesis-s3",
    version     := "0.5.0-rc3",
    description := "Kinesis sink for S3"
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.Libraries.kinesisClient,
      Dependencies.Libraries.kinesisConnector,
      Dependencies.Libraries.slf4j,
      Dependencies.Libraries.hadoop,
      Dependencies.Libraries.elephantbird,
      Dependencies.Libraries.hadoopLZO,
      Dependencies.Libraries.yodaTime,
      // Scala
      Dependencies.Libraries.scopt,
      Dependencies.Libraries.config,
      Dependencies.Libraries.scalaz7,
      Dependencies.Libraries.json4sJackson,
      Dependencies.Libraries.snowplowTracker,
      // Scala (test only)
      Dependencies.Libraries.specs2,
      // Thrift (test only)
      Dependencies.Libraries.collectorPayload
    )
  )

shellPrompt := { _ => "kinesis-s3> " }
