/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.blob.core

import java.net.URI
import java.time.Instant

import org.specs2.Specification

import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

class DynamicPathSpec extends Specification with CatsEffect {

  def is = s2"""
  getFullPath should
    handle a config path with trailing slash $gFP_trailingSlash
    handle a config path without trailing slash $gFP_noTrailingSlash
    handle a config path with some subpath $gFP_subpath
    include filename prefix when provided $gFP_prefix
    include partition when provided $gFP_partition
    clean a path with empty parts $gFP_cleanPath
  getPartition should
    return empty string when partition format is None $gP_none
    resolve time patterns correctly $gP_time
    resolve schema patterns correctly $gP_schema
    handle atomic schema without substitution $gP_schemaAtomic
    handle mixed time and schema patterns $gP_timeAndSchema
    handle multiple occurrences of a same pattern $gP_multipleOccurences
  """

  private val uuidRegex    = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
  private val customSchema = SchemaKey("com.example", "test_event", "jsonschema", SchemaVer.Full(1, 2, 3))
  private val testInstant  = Instant.parse("2023-12-25T15:30:45Z")

  // Tests getFullPath

  def gFP_trailingSlash = {
    val testConfigPath = URI.create("blob://test-dir/")
    val uri = DynamicPath.getFullPath(
      testConfigPath,
      None,
      None,
      testInstant,
      AtomicSchema
    )
    val one   = uri.toString must matching(s"blob://test-dir/2023-12-25-153045-$uuidRegex.gz".r)
    val two   = uri.getScheme must beEqualTo("blob")
    val three = uri.getHost must beEqualTo("test-dir")
    val four  = uri.getPath must matching(s"/2023-12-25-153045-$uuidRegex.gz".r)
    one and two and three and four
  }

  def gFP_noTrailingSlash = {
    val testConfigPath = URI.create("blob://test-dir")
    val uri = DynamicPath.getFullPath(
      testConfigPath,
      None,
      None,
      testInstant,
      AtomicSchema
    )
    uri.toString must matching(s"blob://test-dir/2023-12-25-153045-$uuidRegex.gz".r)
  }

  def gFP_subpath = {
    val testConfigPath = URI.create("blob://test-dir/sub/path")
    val uri = DynamicPath.getFullPath(
      testConfigPath,
      None,
      None,
      testInstant,
      AtomicSchema
    )
    uri.toString must matching(s"blob://test-dir/sub/path/2023-12-25-153045-$uuidRegex.gz".r)
  }

  def gFP_prefix = {
    val testConfigPath = URI.create("blob://test-dir")
    val uri = DynamicPath.getFullPath(
      testConfigPath,
      Some("pre-"),
      None,
      testInstant,
      AtomicSchema
    )
    uri.toString must matching(s"blob://test-dir/pre-2023-12-25-153045-$uuidRegex.gz".r)
  }

  def gFP_partition = {
    val testConfigPath = URI.create("blob://test-dir")
    val uri = DynamicPath.getFullPath(
      testConfigPath,
      None,
      Some("partition"),
      testInstant,
      AtomicSchema
    )
    uri.toString must matching(s"blob://test-dir/partition/2023-12-25-153045-$uuidRegex.gz".r)
  }

  def gFP_cleanPath = {
    val testConfigPath = URI.create("blob://test-dir//")
    val uri = DynamicPath.getFullPath(
      testConfigPath,
      None,
      Some("//partition///"),
      testInstant,
      AtomicSchema
    )
    uri.toString must matching(s"blob://test-dir/partition/2023-12-25-153045-$uuidRegex.gz".r)
  }

  // Tests getPartition

  def gP_none = {
    val result = DynamicPath.getPartition(None, testInstant, AtomicSchema)
    result must beEqualTo("")
  }

  def gP_time = {
    val result = DynamicPath.getPartition(
      Some("year={yyyy}/month={MM}/day={dd}/hour={HH}/minute={mm}/second={ss}"),
      testInstant,
      AtomicSchema
    )
    result must beEqualTo("year=2023/month=12/day=25/hour=15/minute=30/second=45")
  }

  def gP_schema = {
    val result = DynamicPath.getPartition(
      Some("{vendor}/{schema}/{name}/{format}/{model}"),
      testInstant,
      customSchema
    )
    result must beEqualTo("com.example/test_event/test_event/jsonschema/1")
  }

  def gP_schemaAtomic = {
    val result = DynamicPath.getPartition(
      Some("{vendor}.{schema}.{format}"),
      testInstant,
      AtomicSchema
    )
    result must beEqualTo("{vendor}.{schema}.{format}")
  }

  def gP_timeAndSchema = {
    val result = DynamicPath.getPartition(
      Some("{vendor}/{schema}/year={yyyy}/month={MM}"),
      testInstant,
      customSchema
    )
    result must beEqualTo("com.example/test_event/year=2023/month=12")
  }

  def gP_multipleOccurences = {
    val result = DynamicPath.getPartition(
      Some("{vendor}/{vendor}-{schema}/{schema}/month={MM}/month={MM}"),
      testInstant,
      customSchema
    )
    result must beEqualTo("com.example/com.example-test_event/test_event/month=12/month=12")
  }
}
