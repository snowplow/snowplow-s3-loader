# Snowplow S3 Loader

[![Build Status][travis-image]][travis]
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

| Technical Docs             | Setup Guide           | Roadmap              | Contributing                |
|:--------------------------:|:---------------------:|:--------------------:|:---------------------------:|
| ![i1][techdocs-image]      | ![i2][setup-image]    | ![i3][roadmap-image] |![i4][contributing-image]    |
| [Technical Docs][techdocs] | [Setup Guide][config] | [Roadmap][roadmap]   |[Contributing][contributing] |

## Copyright and license

Snowplow S3 Loader is copyright 2014-2023 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[travis-image]: https://travis-ci.org/snowplow/snowplow-s3-loader.png?branch=master
[travis]: http://travis-ci.org/snowplow/snowplow-s3-loader

[release-image]: http://img.shields.io/badge/release-2.2.9-blue.svg?style=flat
[releases]: https://github.com/snowplow/snowplow-s3-loader/releases

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[kinesis]: http://aws.amazon.com/kinesis/
[snowplow]: http://snowplowanalytics.com
[hadoop-lzo]: https://github.com/twitter/hadoop-lzo
[protobufs]: https://github.com/google/protobuf/
[elephant-bird]: https://github.com/twitter/elephant-bird/
[s3]: http://aws.amazon.com/s3/
[sbt]:https://www.scala-sbt.org/

[config]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/s3-loader/configuration-reference/
[techdocs]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/s3-loader/
[roadmap]: https://github.com/snowplow/snowplow/projects/7
[contributing]: https://docs.snowplow.io/docs/contributing/

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png
