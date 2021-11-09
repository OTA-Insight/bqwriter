# bqwriter [![Go Workflow Status](https://github.com/OTA-Insight/bqwriter/workflows/Go/badge.svg)](https://github.com/OTA-Insight/bqwriter/actions/workflows/go.yml)&nbsp;[![GoDoc](https://godoc.org/github.com/OTA-Insight/bqwriter?status.svg)](https://godoc.org/github.com/OTA-Insight/bqwriter)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/OTA-Insight/bqwriter)](https://goreportcard.com/report/github.com/OTA-Insight/bqwriter)&nbsp;[![license](https://img.shields.io/github/license/OTA-Insight/bqwriter.svg)](https://github.com/OTA-Insight/bqwriter/blob/master/LICENSE.txt)&nbsp;[![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/OTA-Insight/bqwriter?include_prereleases)](https://github.com/OTA-Insight/bqwriter/releases)

A Go package to write data into [Google BigQuery](https://cloud.google.com/bigquery/)
concurrently with a high throughput. By default [the InsertAll() API](https://cloud.google.com/bigquery/streaming-data-into-bigquery)
is used (REST API under the hood), but you can configure to use [the Storage Write API](https://cloud.google.com/bigquery/docs/write-api) (GRPC under the hood) as well.

The InsertAll API is easier to configure and can work pretty much out of the box without any configuration.
It is recommended to use the Storage API as it is faster and comes with a lower cost. The latter does however
require a bit more configuration on your side, including a Proto schema file as well.
See [the Storage example below](#Storage-Streamer) on how to do this.

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

We currently support Go versions 1.15 and newer.

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
import (
    "context"

    "github.com/OTA-Insight/bqwriter"
)

// TODO: use more specific context
ctx := context.Background()

// create a BQ (stream) writer thread-safe client,
bqWriter, err := bqwriter.NewStreamer(
    ctx,
    "my-gcloud-project",
    "my-bq-dataset",
    "my-bq-table",
    nil, // use default config
)
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

You build a `Streamer` client using optionally the `StreamerConfig` as you can see in the above example.
The entire config is optional and has sane defaults, but note that there is a lot you can configure in this config prior to actually building the streamer. Please consult the <https://pkg.go.dev/github.com/OTA-Insight/bqwriter#StreamerConfig> for more information.

The `myRow` structure used in this example is one way to pass in the information
of a single row to the `(*Streamer).Write` method. This structure implements the
[`ValueSaver`](https://pkg.go.dev/cloud.google.com/go/bigquery#ValueSaver) interface.
An example of this:

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
import (
    "context"

    "github.com/OTA-Insight/bqwriter"
)

// TODO: use more specific context
ctx := context.Background()

// create a BQ (stream) writer thread-safe client,
bqWriter, err := bqwriter.NewStreamer(
    ctx,
    "my-gcloud-project",
    "my-bq-dataset",
    "my-bq-table",
    &bqwriter.StreamerConfig{
        // use 5 background worker threads
        WorkerCount: 5,
        // ignore errors for invalid/unknown rows/values,
        // by default these errors make a write fail
        InsertAllClient: &bqwriter.InsertAllClientConfig{
             // Write rows fail for invalid/unknown rows/values errors,
             // rather than ignoring these errors and skipping the faulty rows/values.
             // These errors are logged using the configured logger,
             // and the faulty (batched) rows are dropped silently.
            FailOnInvalidRows:    true,
            FailForUnknownValues: true, 
        },
    },
)
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

If you can you should use the StorageStreamer. The InsertAll API is now considered legacy
and is more expensive and less efficient to use compared to the storage API.

Here follows an example on how you can create such a storage API driven BigQuery streamer.

```go
import (
    "context"

    "github.com/OTA-Insight/bqwriter"
    "google.golang.org/protobuf/reflect/protodesc"

    // TODO: define actual path to pre-compiled protobuf Go code
    "path/to/my/proto/package/protodata"
)

// TODO: use more specific context
ctx := context.Background()

// create proto descriptor to use for storage client
protoDescriptor := protodesc.ToDescriptorProto((&protodata.MyCustomProtoMessage{}).ProtoReflect().Descriptor())

// create a BQ (stream) writer thread-safe client,
bqWriter, err := bqwriter.NewStreamer(
    ctx,
    "my-gcloud-project",
    "my-bq-dataset",
    "my-bq-table",
    &bqwriter.StreamerConfig{
        // use 5 background worker threads
        WorkerCount: 5,
        // create the streamer using a Protobuf message encoder for the data
        StorageClient: &bqwriter.StorageClientConfig{
            ProtobufDescriptor: protoDescriptor,
        },
    },
)
)
if err != nil {
    // TODO: handle error gracefully
    panic(err)
}
// do not forget to close, to close all background resources opened
// when creating the BQ (stream) writer client
defer bqWriter.Close()

// TOOD: populate fields of the proto message
msg := new(protodata.MyCustomProtoMessage)

// You can now start writing data to your BQ table
bqWriter.Write(msg)
// NOTE: only write one row at a time using `(*Streamer).Write`,
// multiple rows can be written using one `Write` call per row.
```

You must define the `StorageClientConfig`, as demonstrated in previous example,
in order to be create a Streamer client using the Storage API.
Note that you cannot create a blank `StorageClientConfig` or any kind of default,
as you are required to configure it with either a `bigquery.Schema` or a `descriptorpb.DescriptorProto`,
with the latter being preferred and used of the first.

The schema or Protobuf descriptor are used to be able to encode the data prior to writing
in the correct format as Protobuf encoded binary data.

- `BigQuerySchema` can be used in order to use a data encoder for the StorageClient
  based on a dynamically defined BigQuery schema in order to be able to encode any struct,
  JsonMarshaler, Json-encoded byte slice, Stringer (text proto) or string (also text proto)
  as a valid protobuf message based on the given BigQuery Schema;
- `ProtobufDescriptor` can be used in order to use a data encoder for the StorageClient
  based on a pre-compiled protobuf schema in order to be able to encode any proto Message
  adhering to this descriptor;

`ProtobufDescriptor` is preferred as you might have to pay a performance penalty
should you want to use the `BigQuerySchema` instead.

### Batch client

The batch client can be used if you want to upload a big dataset of data to bigquery without any additional cost.

Here follows an example on how you can create such a batch API driven BigQuery client.

```go
import (
    "bytes"
    "context"
    "encoding/json"

    "github.com/OTA-Insight/bqwriter"

    "cloud.google.com/go/bigquery"
)

func main() {
    ctx := context.Background()
	
    // By using new(bqwriter.BatchClientConfig) we will create a config with bigquery.JSON as default format
    // And the schema will be autodetected via the data.
    // Possible options are: 
    // - BigQuerySchema: Schema to use to upload to bigquery.
    // - SourceFormat: Format of the data we want to send.
    // - FailForUnknownValues: will treat records that have unknown values as invalid records.
    // - WriteDisposition: Defines what the write disposition should be to the bigquery table.
    batchConfig := new(bqwriter.BatchClientConfig)
	
    // create a BQ (stream) writer thread-safe client.
    bqWriter, err := bqwriter.NewStreamer(
        ctx,
        "my-gcloud-project",
        "my-bq-dataset",
        "my-bq-table",
        &bqwriter.StreamerConfig{
            BatchClient: batchConfig
        },
    )

    if err != nil {
        // TODO: handle error gracefully
        panic(err)
    }
		
    // do not forget to close, to close all background resources opened
    // when creating the BQ (stream) writer client
    defer bqWriter.Close()

    // Create some data, make sure your data implements the io.Reader interface so it can be used!
    type person struct {
        ID   int    `json:"id"`
        Name string `json:"name"`
    }
    data := []person{
        {
            ID:   1,
            Name: "John",
        },
        {
            ID:   2,
            Name: "Jane",
        },
    }

    // Convert the data into a reader.
    var buffer bytes.Buffer
    for _, r := range data {
        body, _ := json.Marshal(r)
        buffer.Write(body)
        buffer.WriteString("\n") // If the format is JSON, make sure every row is on a newline.
    }

    // Write the data to bigquery.
    _, err := bqWriter.Put(bytes.NewReader(buffer.Bytes()))
    if err != nil {
        // TODO: handle error gracefully
        panic(err)
    }
}
```

You must define the `BatchClientConfig`, as demonstrated in previous example,
in order to create a Batch client.
Note that you cannot create a blank `BatchClientConfig` or any kind of default,
as you are required to configure it with at least a `SourceFormat`.


**BatchClientConfig options**

- `BigQuerySchema` BigQuerySchema can be used in order to use a data encoder for the batchClient
  based on a dynamically defined BigQuery schema in order to be able to encode any struct,
  JsonMarshaler, Json-encoded byte slice, Stringer (text proto) or string (also text proto)
  as a valid protobuf message based on the given BigQuery Schema.
  
  The `BigQuerySchema` is required for all `SourceFormat` except for `bigquery.CSV` and `bigquery.JSON` as these
  2 formats will autodetect the schema via the content.

- `SourceFormat` is used to define the format that the data is that we will send.
  Possible options are:
  - `bigquery.CSV`
  - `bigquery.Avro`
  - `bigquery.JSON`
  - `bigquery.Parquet`
  - `bigquery.ORC`

- `FailForUnknownValues` causes records containing such values
  to be treated as invalid records.
  
  Defaults to false, making it ignore any invalid values, silently ignoring these errors,
  and publishing the rows with the unknown values removed from them.

- `WriteDisposition` can be used to define what the write disposition should be to the bigquery table.
  Possible options are:
    - bigquery.WriteAppend
    - bigquery.WriteTruncate
    - bigquery.WriteEmpty
  
  Defaults to bigquery.WriteAppend, which will append the data to the table.

#### Future improvements
Currently, the package does not support any additional options that the different `SourceFormat` could have, feel free to
open a feature request to add support for these.

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
if there is sufficient interest for it. The [Contributing section](#Contributing) section explains how you can actively
help to get this supported if desired.

## Instrumentation

We currently support the ability to implement your logger which can be used instead of the standard logger which prints
to STDERR. It is used for debug statements as well as unhandled errors. Debug statements aren't used everywhere, any unhandled error that isn't propagated is logged using the used logger.

> You can find the interface you would need to implement to support your own Logger at
> <https://godoc.org/github.com/OTA-Insight/bqwriter/log#Logger>.

Any other instrumentation used for monitoring such as statistics or tags are currently not supported.
The original experimental storage client of Google supported <https://github.com/census-instrumentation/opencensus-go> out of the box and this support couldn't even be opted out of. Please create a fully detailed and well motivated
proposal should you find yourself in need of the ability for the library to collect stats.

If it would be supported it would be an opt-in feature which can be enabled by implementing a kind of interface,
such that this library doesn't have to include any dependency for it and doesn't bind itself to any specific kind
of monitoring tool. Please include your perspective and theoretical interface as part of your proposal.

We do not have any need for this feature ourselves and thus will only implement it in case there
is sufficient and well motivated interest for it.

## Contributing

Contributions are welcome. Please, see the [CONTRIBUTING](/CONTRIBUTING.md) document for details.

Please note that this project is released with a Contributor Code of Conduct.
By participating in this project you agree to abide by its terms.
See [Contributor Code of Conduct](/CONTRIBUTING.md#contributor-code-of-conduct) for more information.
