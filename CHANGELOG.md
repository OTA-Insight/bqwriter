# Changes

## [v0.6.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.0...v0.5.1) (2021-11-12)

Documentation improvements:

- Rewrite the batch-driven Streamer README example to stay true to its purpose and
  reflect closer the real world scenarios it is meant to serve in (TODO);
- Refactor internal benchmark package as a stand-alone binary,
  and rename it to integration:
  - The latter because it was from the beginning used
    as integration dev-triggered tests rather than a true benchmark;
  - And the first to allow more flexibility and differentiation in how we test what streamer configuration;
- Add developer instructions to the README;

Bug Fixes:

- Storage Streamer Client couldn't be closed without hanging, now it can be closed;
- Storage Streamer Client can now be used with BigQuery.Schema, previously it would result in a schema name error;
- Storage Streamer Client now demoted Canceled/Unavailable code errors for append rows to debug logs: as these are related
  to underlying connections being reset or a similar kind of EOF event;
- improve & clean up integration tests;

## [v0.5.1](https://www.github.com/OTA-Insight/bqwriter/compare/v0.5.1...v0.5.0) (2021-11-10)

Update storage API documentation & end-to-end tests:

- storage writer API expects proto2 semantics, proto3 shouldn't be used (yet);
- the [normalizeDescriptor](https://pkg.go.dev/cloud.google.com/go/bigquery/storage/managedwriter/adapt#NormalizeDescriptor)
  should be used to get a descriptor with nested types in order to have it work nicely with nested types;
- The proto well-known types aren't yet properly supported,
  and Timestamp is among them. The public docs have a section on wire format conversions:
  https://cloud.google.com/bigquery/docs/write-api#data_type_conversions.
  - Short answer: use an int64 with epoch-micros for fields that have the Timestamp type...
    and this instead of `import "google/protobuf/timestamp.proto"`;
- Batch client supports auto-detection of BigQuery schema for CSV and Json source formats,
  with Json you have to be aware in that case however that you match the casing exactly,
  as otherwise it will complain about duplicate case-insensitive fields, go figure...

Bug Fixes:

- a couple of error logs used wrongfully the directive `%w` for errors instead of `%v`,
  this has now been corrected and should result in cleaner logs;

## [v0.5.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.5.0...v0.4.1) (2021-11-09)

- add batch upload support (https://cloud.google.com/bigquery/docs/batch-loading-data),
  this is a third client next to the already supported InsertAll (legacy) and storage API clients,
  fixes issue #2 with PR #5;
- add benchmark code (mostly as end-to-end tests) with production-ready Google Cloud infrastructure, load and data;
- remove forked managedwriter code (fixes issue #4):
  - managedwriter is still in active development and having to maintain our own copy would be almost a project on itself;
  - author seems to be willing to fix our issues where appropriate;
  - author also is willing to promote this package to the official bigquery Golang API.
    There is no time promise here, the only condition seems to be that the author has to be happy with the API signatures;
  - there are a couple of open issues tracked for the official Google managedwriter which remain unresolved:
    - make GRPC/ManagedWriter stats tracking opt-in or configurable: https://github.com/googleapis/google-cloud-go/issues/5100;
    - document and support correctly nested proto types for the storage API client: https://github.com/googleapis/google-cloud-go/issues/5097;
    - be able to configure and globally limit the gax.Retryer used for the storage API client: https://github.com/googleapis/google-cloud-go/issues/5094;

Bug Fixes:

- storage API:
  - fixes related to EOF errors are fixed by switching over to the latest version of the BQ Storage managedwriter;
- logger:
  - std logger (STDERR) was logging without the use of newlines to separate log statements;

## [v0.4.1](https://www.github.com/OTA-Insight/bqwriter/compare/v0.4.0...v0.4.1) (2021-12-06)

- add storage API driven stream examples to README;
- fix nil deref bug when creating streamer using StorageAPI;

## [v0.4.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.3.1...v0.4.0) (2021-11-05)

- refactor code:
  - all internal code is now found in one of the internal packages,
    as to have a cleaner codebase and keeping all its definitions explicitly internal;
  - added internal BQ Storage client, which is a heavily modified fork from
    <https://github.com/googleapis/google-cloud-go/tree/a2af4de215a42848368ec3081263d34782032caa/bigquery/storage/managedwriter>;
    - the fork is only meant to be as long as required, it is desired to switch to the upstream
      managed writer as soon as possible;
    - Only using the default stream is supported. CommittedStream and/or PendingStream
      can be supported upon request;
  - constants are now moved to the `constant` package of this module as to make it very clear
    within the code that these are constants as well as to allow the ability for both the internal
    as well as the public root package to make use of it;
  - the `Logger` interface is moved to its own `log` package for the same reasons as the
    introduction of the `constant` package;
- adds initial StorageAPI Support:
    - Only using the default stream is supported. CommittedStream and/or PendingStream
      can be supported upon request;
- bump min Go version supported to Go 1.15, as we make use of the `time.Ticket.Reset` functionality
  which is only available sine Go 1.15:
    - Note this feature isn't critical so if ever required for a good reason,
      we can probably work around it and downgrade the min Go version once again;
- updated dependencies to latest:
  - google.golang.org/grpc: v1.42.0;

Other updates made to the repository:

- enforce issue templates in the OTA-Insight/bqwriter GitHub project;
- add a pull request template in the OTA-Insight/bqwriter GitHub project;
- rename this file to [CHANGELOG.md](CHANGELOG.md) (was CHANGES.md) in order to better reflect the usual conventions;
- add other conventional special files: [AUTHORS](AUTHORS) and [CODEOWNERS](CODEOWNERS);

## [v0.3.1](https://www.github.com/OTA-Insight/bqwriter/compare/v0.3.0...v0.3.1) (2021-10-28)

- fix README.md badges and rename LICENSE to LICENSE.txt;
- updated dependencies to latest:
  - golang.org/x/*: 2021-10-20+;
  - google.golang.org/api: v0.59.0;

## [v0.3.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.2.0...v0.3.0) (2021-10-25)

- remove unused WriteRetryConfig (its use was eliminated in v0.2.0);
- fix a linter issue found in `v0.2.0`'s `streamer.go` codebase (indention);
- add golint-ci dev + CI support for better code quality;

## [v0.2.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.1.0...v0.2.0) (2021-10-19)

- remove exponential back off logic from insertAll driven streamer client,
  as this logic is already built-in the std BQ client used internally;
  - we do still keep the max deadline on top of that by using a deadline context;
- remove the builder-pattern approach used to build a streamer,
  and instead use a clean Config approach, as to keep it as simple as possible,
  while at the same time being more Go idiomatic;
- upgrade `google.golang.org/grpc` to `v1.41.0`, was on `v1.40.0`;

## v0.1.0 (2021-10-18)

Initial pre-release version.
Not yet ready for production-use.

This version is already used for internal projects
at OTA Insight, mostly for testing purposes.

- provide a small API (`Streamer`) to write rows concurrently to a specific BQ table;
- the client within this API can be build (`StreamerBuilder`) using a builder with sane defaults;
- most configurations can be optionally configured where desired;
- dependencies are kept to the bare minimum google cloud dependencies,
  with no other third party dependencies required;
