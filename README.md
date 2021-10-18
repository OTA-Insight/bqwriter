# bqwriter [![Go Workflow Status](https://github.com/OTA-Insight/bqwriter/workflows/Go/badge.svg)](https://github.com/OTA-Insight/bqwriter/actions/workflows/go.yml)&nbsp;[![GoDoc](https://godoc.org/github.com/OTA-Insight/bqwriter?status.svg)](https://godoc.org/github.com/OTA-Insight/bqwriter)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/OTA-Insight/bqwriter)](https://goreportcard.com/report/github.com/OTA-Insight/bqwriter)&nbsp;[![license](https://img.shields.io/github/license/OTA-Insight/bqwriter.svg)](https://github.com/OTA-Insight/bqwriter/blob/master/LICENSE.txt)

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

## Examples

In this section you'll find some quick examples to help you get started
together with the official documentation which you can find at <https://pkg.go.dev/github.com/OTA-Insight/bqwriter>.

The `Streamer` client is safe for concurrent use and can be used from as many go routines as you wish.
No external locking or other concurrency-safe mechanism is required from your side. To keep these examples
as small as possible however they are written in a linear synchronous fashion, but it is encouraged to use the
`Streamer` client from multiple go routines, in order to be able to write rows at a sufficiently high throughput.

Please also note that errors are not handled gracefully in these examples as ot keep them small and narrow in scope.

### Basic InsertAll Streamer

```go
import "github.com/OTA-Insight/bqwriter"

// create stream builder, used to configure the BQ (stream) writer
bqWriterBuilder, err := bqwriter.NewStreamerBuilder(
    "my-gcloud-project",
    "my-bq-dataset",
    "my-bq-table",
)
if err != nil {
    // TODO: handle error gracefully
    panic(err)
}

// build the BQ (stream) writer client
ctx := context.Background()  // TODO: use more specific context
bqWriter, err = bqWriterBuilder.BuildStreamer(ctx)
if err != nil {
    // TODO: handle error gracefully
    panic(err)
}
// do not forget to close, to close all background resources opened
// when creating the BQ (stream) writer client
defer bqWriter.Close()

// You can now start writing data to your BQ table
bqWriter.Write(&myRow{Timestamp: time.UTC().Now(), Username: "test"})
// NOTE: only write one row at a time using `(*Streamer).Write`,
// multiple rows can be written using one `Write` call per row.
```

You build a `Streamer` client using the `StreamerBuilder` as you can see in the above example.
Note that there is a lot you can still configure in the Streamer builder (`bqWriterBuilder`) prior to actually
building the streamer. Please consult the <https://pkg.go.dev/github.com/OTA-Insight/bqwriter#StreamerBuilder> for more information.

The `myRow` structure used in this example is one way to pass in the information of a single row to the `(*Streamer).Write` method.
This structure implements the [`ValueSaver`](https://pkg.go.dev/cloud.google.com/go/bigquery#ValueSaver) interface. An example of this:

```go
import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type myRow struct {
	Timestamp time.Time
	Username  string
}

func (mr *myRow) Save() (row map[string]bigquery.Value, insertID string, err error) {
	return map[string]bigquery.Value{
		"timestamp": civil.DateTimeOf(rr.Timestamp),
		"username":  mr.Username,
	}, "", nil
}
```

You can also pass in a `struct` directly and the schema will be inferred automatically
based on its public items. This flexibility has a runtime cost by having to apply reflection.

A raw `struct` can also be stored by using the [`StructSaver`](https://pkg.go.dev/cloud.google.com/go/bigquery#StructSaver) interface,
in which case you get the benefit of being able to write any kind of `struct` while at the same time being able to pass
in the to be used scheme already such that it doesn't have to be inferred and giving you exact controls for each field on top of that.

If you have the choice however than we do recommend to implement the `ValueSaver` for your row `struct` as this gives you the best of both worlds,
while at the same time also giving you the easy built-in ability to define a unique `insertID` per row which will help prevent potential duplicates
that can otherwise happen while retrying to write rows which have failed temporarily.

### Custom InsertAll Streamer

Using the same `myRow` structure from previous example,
here is how we create a `Streamer` client with a more
custom configuration:

```go
import "github.com/OTA-Insight/bqwriter"

// create stream builder, used to configure the BQ (stream) writer
bqWriterBuilder, err := bqwriter.NewStreamerBuilder(
    "my-gcloud-project",
    "my-bq-dataset",
    "my-bq-table",
)
if err != nil {
    // TODO: handle error gracefully
    panic(err)
}

// build the BQ (stream) writer client
ctx := context.Background()  // TODO: use more specific context
bqWriter, err = bqWriterBuilder.
    // use 5 background worker threads,
    WorkerCount(5).
    // ignore errors for invalid/unknown rows/values,
    // by default these errors make a write fail
    ClientInsertAll(true, true).
    // disable retrying on errors
    WriteRetryConfig(&bqwriter.WriteRetryConfig{
        MaxRetries: -1,
    }).
    // build the streamer using your custom configuration
    BuildStreamer(ctx)
if err != nil {
    // TODO: handle error gracefully
    panic(err)
}
// do not forget to close, to close all background resources opened
// when creating the BQ (stream) writer client
defer bqWriter.Close()

// You can now start writing data to your BQ table
bqWriter.Write(&myRow{Timestamp: time.UTC().Now(), Username: "test"})
// NOTE: only write one row at a time using `(*Streamer).Write`,
// multiple rows can be written using one `Write` call per row.
```

### Storage Streamer

TODO

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
