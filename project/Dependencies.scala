/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
import sbt._

object Dependencies {
  val resolvers = Seq(
    ("Snowplow Analytics Maven releases repo" at "http://maven.snplow.com/releases/").withAllowInsecureProtocol(true),
    "Snowplow Bintray Maven repo"            at "https://snowplow.bintray.com/snowplow-maven",
    "Twitter" at "https://maven.twttr.com/"
  )

  object V {
    // Java
    val slf4j            = "1.7.30"
    val log4j            = "2.14.0"
    val kinesisClient    = "1.14.7"
    val kinesisConnector = "1.3.0"
    val hadoop           = "2.7.7"
    val elephantbird     = "4.17"
    val hadoopLZO        = "0.4.20"
    val apacheCommons    = "3.2.1"
    val jackson          = "2.12.6"
    val sentry           = "1.7.30"
    val collections      = "3.2.2" // Address vulnerability
    val jaxbApi          = "2.3.1"
    // Thrift (test only)
    val collectorPayload = "0.0.0"
    val thrift           = "0.15.0" // Address vulnerabilities
    // Scala
    val decline         = "2.0.0"
    val circe           = "0.13.0"
    val snowplowTracker = "0.7.0"
    val snowplowBadrows = "2.1.0"
    val pureconfig      = "0.14.1"
    val igluCore        = "1.0.0"
    // Scala (test only)
    val specs2          = "4.10.5"
  }

  object Libraries {
    // Java
    val slf4j            = "org.slf4j"                        %  "slf4j-simple"              % V.slf4j
    val jclOverSlf4j     = "org.slf4j"                        %  "jcl-over-slf4j"            % V.slf4j
    val kinesisClient    = "com.amazonaws"                    %  "amazon-kinesis-client"     % V.kinesisClient
    val kinesisConnector = "com.amazonaws"                    %  "amazon-kinesis-connectors" % V.kinesisConnector
    val jacksonCbor      = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"    % V.jackson
    val jackson          = "com.fasterxml.jackson.core"       % "jackson-databind"           % V.jackson
    val thrift           = "org.apache.thrift"                % "libthrift"                  % V.thrift
    val hadoop           = ("org.apache.hadoop"               %  "hadoop-common"             % V.hadoop)
      .exclude("org.slf4j", "slf4j-log4j12")
      .exclude("commons-beanutils", "commons-beanutils")
      .exclude("commons-beanutils", "commons-beanutils-core")
      .exclude("commons-collections", "commons-collections")
      .exclude("commons-logging", "commons-logging")
      .exclude("org.apache.htrace", "htrace-core")
      .exclude("junit", "junit")
      .exclude("org.apache.zookeeper", "zookeeper")
      .exclude("org.apache.hadoop", "hadoop-auth")
      .exclude("org.apache.curator", "curator-client")
      .exclude("log4j", "log4j")
      .exclude("com.google.code.gson", "gson")
      .exclude("org.apache.avro", "avro")
      .exclude("org.codehaus.jackson", "jackson-mapper-asl")
      .exclude("com.sun.jersey", "jersey-json")
      .exclude("org.mortbay.jetty", "jetty-sslengine")
      .exclude("org.mortbay.jetty", "jetty-util")
      .exclude("org.mortbay.jetty", "jetty")
    val collections      = "commons-collections"              % "commons-collections"                % V.collections
    val jaxbApi          = "javax.xml.bind"                   % "jaxb-api"                           % V.jaxbApi
    val elephantbird     = ("com.twitter.elephantbird"        %  "elephant-bird-core"                % V.elephantbird)
      .exclude("com.hadoop.gplcompression", "hadoop-lzo")
    val hadoopLZO        = "com.hadoop.gplcompression"        %  "hadoop-lzo"                        % V.hadoopLZO
    val apacheCommons    = "org.apache.directory.studio"      % "org.apache.commons.collections"     % V.apacheCommons
    val sentry           = "io.sentry"                        % "sentry"                             % V.sentry

    val decline          = "com.monovore"                     %% "decline"                           % V.decline
    val circe            = "io.circe"                         %% "circe-generic"                     % V.circe
    val snowplowTracker  = "com.snowplowanalytics"            %% "snowplow-scala-tracker-emitter-id" % V.snowplowTracker
    val snowplowBadrows  = "com.snowplowanalytics"            %% "snowplow-badrows"                  % V.snowplowBadrows
    val pureconfig       = "com.github.pureconfig"            %% "pureconfig"                        % V.pureconfig
    val pureconfigCirce  = "com.github.pureconfig"            %% "pureconfig-circe"                  % V.pureconfig

    val specs2           = "org.specs2"                       %% "specs2-core"                       % V.specs2           % Test
    val collectorPayload = "com.snowplowanalytics"            %  "collector-payload-1"               % V.collectorPayload % Test
  }
}
