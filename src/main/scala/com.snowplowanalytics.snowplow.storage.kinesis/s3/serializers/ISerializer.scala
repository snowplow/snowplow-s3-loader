package com.snowplowanalytics.snowplow.storage.kinesis.s3.serializers

import java.io.ByteArrayOutputStream
import com.snowplowanalytics.snowplow.storage.kinesis.s3.EmitterInput

case class NamedStream(filename: String, stream: ByteArrayOutputStream)

case class SerializationResult(namedStreams: List[NamedStream], results: List[EmitterInput])

trait ISerializer {
  def serialize(records: List[ EmitterInput ], baseFilename: String): SerializationResult
}
