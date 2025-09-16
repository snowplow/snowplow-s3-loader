# Snowplow S3 Loader

[![Release][release-image]][releases]
[![License][license-image]][license]

## Overview

Snowplow S3 Loader consumes records from an [Amazon Kinesis][kinesis] stream and writes them to S3.

Files are compressed with `gzip`.
Records are treated as byte arrays containing UTF-8 encoded strings.
New lines are used to separate records written to a file.

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
[snowplow]: http://snowplow.io

[setup]: https://docs.snowplow.io/docs/api-reference/loaders-storage-targets/s3-loader/configuration-reference/
[techdocs]: https://docs.snowplow.io/docs/api-reference/loaders-storage-targets/s3-loader/