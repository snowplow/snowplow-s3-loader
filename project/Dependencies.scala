/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd. All rights reserved.
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
    val awsSdk           = "1.12.279"
    val kinesisClient    = "1.14.8"
    val kinesisConnector = "1.3.0"
    val hadoop           = "3.3.3"
    val elephantbird     = "4.17"
    val hadoopLZO        = "0.4.20"
    val jackson          = "2.14.1"
    val sentry           = "1.7.30"
    val collections      = "3.2.2" // Address vulnerability
    val jaxbApi          = "2.3.1"
    val protobuf         = "3.21.12"
    val reload4j         = "1.2.22" // Address vulnerability
    // Thrift (test only)
    val collectorPayload = "0.0.0"
    val thrift           = "0.15.0" // Address vulnerabilities
    // Scala
    val decline         = "2.0.0"
    val circe           = "0.13.0"
    val snowplowTracker = "0.7.0"
    val snowplowBadrows = "2.1.0"
    val pureconfig      = "0.15.0"
    val igluCore        = "1.0.0"
    // Scala (test only)
    val specs2          = "4.10.5"
  }

  object Libraries {
    // Java
    val slf4j            = "org.slf4j"                        %  "slf4j-simple"                 % V.slf4j
    val jclOverSlf4j     = "org.slf4j"                        %  "jcl-over-slf4j"               % V.slf4j
    val kinesisClient    = "com.amazonaws"                    %  "amazon-kinesis-client"        % V.kinesisClient
    val kinesisConnector = "com.amazonaws"                    %  "amazon-kinesis-connectors"    % V.kinesisConnector
    val sts              = "com.amazonaws"                    %  "aws-java-sdk-sts"             % V.awsSdk           % Runtime
    val jacksonCbor      = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"       % V.jackson
    val jackson          = "com.fasterxml.jackson.core"       % "jackson-databind"              % V.jackson
    val thrift           = "org.apache.thrift"                % "libthrift"                     % V.thrift
    val hadoopMapReduce  = "org.apache.hadoop"                %  "hadoop-mapreduce-client-core" % V.hadoop
    val hadoop           = "org.apache.hadoop"                %  "hadoop-common"                % V.hadoop
    val protobuf         = "com.google.protobuf"              % "protobuf-java"                 % V.protobuf
    val reload4j         = "ch.qos.reload4j"                  % "reload4j"                      % V.reload4j

    val collections      = "commons-collections"              % "commons-collections"                % V.collections
    val jaxbApi          = "javax.xml.bind"                   % "jaxb-api"                           % V.jaxbApi       % Runtime
    val elephantbird     = ("com.twitter.elephantbird"        %  "elephant-bird-core"                % V.elephantbird)
      .exclude("com.hadoop.gplcompression", "hadoop-lzo")
    val hadoopLZO        = ("com.hadoop.gplcompression"       %  "hadoop-lzo"                        % V.hadoopLZO)
      .excludeAll(ExclusionRule(organization = "org.apache.hadoop"))
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

  val mainDependencies = Seq(
      // Java
      Libraries.kinesisClient,
      Libraries.kinesisConnector,
      Libraries.sts,
      Libraries.jacksonCbor,
      Libraries.slf4j,
      Libraries.jclOverSlf4j,
      Libraries.jackson,
      Libraries.sentry,
      Libraries.jaxbApi,
      Libraries.protobuf,
      Libraries.reload4j,
      // Scala
      Libraries.decline,
      Libraries.circe,
      Libraries.snowplowTracker,
      Libraries.snowplowBadrows,
      Libraries.pureconfig,
      Libraries.pureconfigCirce,
      // Scala (test only)
      Libraries.specs2,
      // Thrift (test only)
      Libraries.collectorPayload,
      Libraries.thrift % Test
  )

  val lzoDependencies = Seq(
      Libraries.hadoop,
      Libraries.hadoopMapReduce,
      Libraries.elephantbird,
      Libraries.hadoopLZO,
      Libraries.thrift,
      Libraries.collections,
      Libraries.jacksonCbor,
  )

  val mainExclusions = Seq(
    ExclusionRule(organization = "commons-logging", name = "commons-logging"),
  )

  val hadoopExclusions = mainExclusions ++ Seq(
    ExclusionRule(organization = "org.apache.avro",   name = "avro"),
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-hdfs-client"),
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-yarn-client"),
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-yarn-api"),
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-auth"),
    ExclusionRule(organization = "org.slf4j",         name = "slf4j-reload4j"),
    ExclusionRule(organization = "org.apache.hadoop.thirdparty", name = "hadoop-shaded-protobuf_3_7"),
    ExclusionRule(organization = "com.sun.jersey"),
    ExclusionRule(organization = "com.sun.jersey.contribs"),
    ExclusionRule(organization = "com.fasterxml.jackson.jaxrs"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.apache.zookeeper"),
    ExclusionRule(organization = "jakarta.activation"),
    ExclusionRule(organization = "jakarta.xml.bind"),
  )

}
