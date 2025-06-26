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

import com.snowplowanalytics.s3.loader.{Config, S3Loader}
import com.snowplowanalytics.s3.loader.serializers.{GZipSerializer, ISerializer}

object S3LoaderWithLzo extends S3Loader {

  override def serializer(config: Config): ISerializer =
    config.output.s3.compression match {
      case Config.Compression.Lzo  => LzoSerializer
      case Config.Compression.Gzip => GZipSerializer
    }

}
