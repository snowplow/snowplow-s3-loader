# Snowplow S3 Loader

[![Release][release-image]][releases]
[![License][license-image]][license]

## Overview

The Snowplow S3 Loader consumes records from an [Amazon Kinesis][kinesis] stream and writes them to S3.

There are 2 file formats supported:
 * LZO
 * GZip

### LZO

The records are treated as raw byte arrays. [Elephant Bird's][elephant-bird] `BinaryBlockWriter` class is used to serialize them as a [Protocol Buffers][protobufs] array (so it is clear where one record ends and the next begins) before compressing them.

The compression process generates both compressed .lzo files and small .lzo.index files ([splittable LZO][hadoop-lzo]). Each index file contain the byte offsets of the LZO blocks in the corresponding compressed file, meaning that the blocks can be processed in parallel.

### GZip

The records are treated as byte arrays containing UTF-8 encoded strings (whether CSV, JSON or TSV). New lines are used to separate records written to a file. This format can be used with the Snowplow Kinesis Enriched stream, among other streams.

## Find out more

| Technical Docs             | Setup Guide           |
|:--------------------------:|:---------------------:|
| [Technical Docs][techdocs] | [Setup Guide][setup]  |

## Copyright and license

Snowplow S3 Loader is copyright 2014-present Snowplow Analytics Ltd. All rights reserved.

Licensed under the [Snowplow Limited Use License Agreement][license]. _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions][faq].)_

[release-image]: https://img.shields.io/github/v/release/snowplow/snowplow-s3-loader?sort=semver&style=flat
[releases]: https://github.com/snowplow/snowplow-s3-loader/releases

[license-image]: https://img.shields.io/badge/license-Snowplow--Limited--Use-blue.svg?style=flat
[license]: https://docs.snowplow.io/limited-use-license-1.1/
[faq]: https://docs.snowplow.io/docs/contributing/limited-use-license-faq/

[kinesis]: http://aws.amazon.com/kinesis/
[snowplow]: http://snowplowanalytics.com
[hadoop-lzo]: https://github.com/twitter/hadoop-lzo
[protobufs]: https://github.com/google/protobuf/
[elephant-bird]: https://github.com/twitter/elephant-bird/

[setup]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/s3-loader/configuration-reference/
[techdocs]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/s3-loader/

