/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.s3.loader
package serializers

// Java libs
import java.io.{ByteArrayOutputStream, DataOutputStream}

// Java lzo
import org.apache.hadoop.conf.Configuration
import com.hadoop.compression.lzo.LzopCodec

// Elephant bird
import com.twitter.elephantbird.mapreduce.io.RawBlockWriter

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
    val lzoOutputStream = lzoCodec.createIndexedOutputStream(outputStream, new DataOutputStream(indexOutputStream))

    val rawBlockWriter = new RawBlockWriter(lzoOutputStream)

    // Populate the output stream with records
    val results = records.map {
      case Right(r) => serializeRecord(r, rawBlockWriter, (rbw: RawBlockWriter) => rbw.write(r))
      case Left(b) => Left(b)
    }

    rawBlockWriter.close()

    val namedStreams = List(ISerializer.NamedStream(s"$baseFilename.lzo", outputStream), ISerializer.NamedStream(s"$baseFilename.lzo.index", indexOutputStream))

    ISerializer.Serialized(namedStreams, results)
  }
}
