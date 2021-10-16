# bqwriter [![Go Workflow Status](https://github.com/OTA-Insight/bqwriter/workflows/Go/badge.svg)](https://github.com/OTA-Insight/bqwriter/actions?query=workflow%Go)&nbsp;[![Coverage Status](https://coveralls.io/repos/github/OTA-Insight/bqwriter/badge.svg?branch=main)](https://coveralls.io/github/OTA-Insight/bqwriter?branch=main)&nbsp;[![GoDoc](https://godoc.org/github.com/OTA-Insight/bqwriter?status.svg)](https://godoc.org/github.com/OTA-Insight/bqwriter)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/OTA-Insight/bqwriter)](https://goreportcard.com/report/github.com/OTA-Insight/bqwriter)&nbsp;[![license](https://img.shields.io/github/license/OTA-Insight/bqwriter.svg)](https://github.com/OTA-Insight/bqwriter/blob/master/LICENSE.txt)

A Go package to write data into [Google BigQuery](https://cloud.google.com/bigquery/)
concurrently with a high throughput. By default [the InsertAll() API](https://cloud.google.com/bigquery/streaming-data-into-bigquery)
is used (REST API under the hood), but you can configure to use [the Storage Write API](https://cloud.google.com/bigquery/docs/write-api) (GRPC under the hood) as well.

The InsertAll API is easier to configure and can work pretty much out of the box without any configuration.
It is recommended to use the Storage API as it is faster and comes with a lower cost. The latter does however
require a bit more configuration on your side, including a Proto schema file as well. See the Storage example below on how to do (TODO).

```go
import "github.com/OTA-Insight/bqwriter"
```

To install the packages on your system, do not clone the repo. Instead:

1. Change to your project directory:

```bash
cd /path/to/my/project
```

2. Get the package using the official Go tooling, which will also add it to your `Go.mod` file for you:

```bash
go get github.com/OTA-Insight/bqwriter
```

NOTE: This package is under development, and may occasionally make backwards-incompatible changes.

## Go Versions Supported

We currently support Go versions 1.13 and newer.

## Authorization

The streamer client will use [Google Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials) for authorization credentials used in calling the API endpoints.
This will allow your application to run in many environments without requiring explicit configuration.

Please open an issue should you require more advanced forms of authorization. The issue should come with an example,
a clear statement of intention and motivation on why this is a useful contribution to this package. Even if you wish
to contribute to this project by implementing this patch yourself, it is none the less best to create an issue prior to it,
such that we can all be aligned on the specifics. Good communication is key here.

It was a choice to not support these advanced authorization methods for now. The reasons being that the package
authors didn't have a need for it and it allowed to keep the API as simple and small as possible. There however some
advanced authorizations still possible:

- Authorize using [a custom Json key file path](https://cloud.google.com/iam/docs/creating-managing-service-account-keys);
- Authorize with more control by using the [`https://pkg.go.dev/golang.org/x/oauth2`](https://pkg.go.dev/golang.org/x/oauth2) package
  to create an `oauth2.TokenSource`;

To conclude. We currently do not support advanced ways for Authorization, but we're open to include support for these,
if there is sufficient interest for it.

## Contributing

Contributions are welcome. Please, see the [CONTRIBUTING](/CONTRIBUTING.md) document for details.

Please note that this project is released with a Contributor Code of Conduct.
By participating in this project you agree to abide by its terms.
See [Contributor Code of Conduct](/CONTRIBUTING.md#contributor-code-of-conduct) for more information.
