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
package com.snowplowanalytics.s3.loader.lzo

// Java libs
import java.io.{ByteArrayOutputStream, DataOutputStream}

// Java lzo
import org.apache.hadoop.conf.Configuration
import com.hadoop.compression.lzo.LzopCodec

// Elephant bird
import com.twitter.elephantbird.mapreduce.io.RawBlockWriter

import com.snowplowanalytics.s3.loader.Result
import com.snowplowanalytics.s3.loader.serializers.ISerializer

/**
 * Object to handle LZO compression of raw events
 */
object LzoSerializer extends ISerializer {

  val lzoCodec = new LzopCodec()
  val conf = new Configuration()
  conf.set("io.compression.codecs", classOf[LzopCodec].getName)
  lzoCodec.setConf(conf)

  def serialize(records: List[Result], baseFilename: String): ISerializer.Serialized = {

    val indexOutputStream = new ByteArrayOutputStream()
    val outputStream = new ByteArrayOutputStream()

    // This writes to the underlying outputstream and indexoutput stream
    val lzoOutputStream = lzoCodec.createIndexedOutputStream(
      outputStream,
      new DataOutputStream(indexOutputStream)
    )

    val rawBlockWriter = new RawBlockWriter(lzoOutputStream)

    // Populate the output stream with records
    val results = records.map {
      case Right(r) =>
        serializeRecord(r, rawBlockWriter, (rbw: RawBlockWriter) => rbw.write(r))
      case Left(b) => Left(b)
    }

    rawBlockWriter.close()

    val namedStreams = List(
      ISerializer.NamedStream(s"$baseFilename.lzo", outputStream),
      ISerializer.NamedStream(s"$baseFilename.lzo.index", indexOutputStream)
    )

    ISerializer.Serialized(namedStreams, results)
  }
}
