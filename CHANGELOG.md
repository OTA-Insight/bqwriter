# Changes

## [v0.6.24](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.23...v0.6.24) (2022-07-04)

Go Version Support:

- min version is now Go 1.17 (was 1.15);
- max version is now Go 1.18 (was 1.17);
  - Note that latest stable release should always work,
    but this is the max version that is being tested as part of our CI pipeline;

Upgraded Dependencies:

- `cloud.google.com/go`: v0.102.1 => v0.103.0
- `golang.org/x/net`: v0.0.0-20220624214902-1bab6f366d9e => v0.0.0-20220630215102-69896b714898
- `golang.org/x/oauth2`: v0.0.0-20220622183110-fd043fe589d2 => v0.0.0-20220630143837-2104d58473e0
- `golang.org/x/sys`: v0.0.0-20220624220833-87e55d714810 => v0.0.0-20220702020025-31831981b65f
- `google.golang.org/api`: v0.85.0 => v0.86.0
- `google.golang.org/genproto`: v0.0.0-20220624142145-8cd45d7dbd1f => v0.0.0-20220630174209-ad1d48641aa7

## [v0.6.23](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.22...v0.6.23) (2022-06-27)

Upgraded Dependencies:

- `cloud.google.com/go/bigquery`: v1.33.0 => v1.34.1
- `github.com/google/uuid`: v1.1.2 => v1.3.0
- `golang.org/x/net`: v0.0.0-20220617184016-355a448f1bc9 => v0.0.0-20220624214902-1bab6f366d9e
- `golang.org/x/oauth2`: v0.0.0-20220608161450-d0670ef3b1eb => v0.0.0-20220622183110-fd043fe589d2
- `golang.org/x/sys`: v0.0.0-20220615213510-4f61da869c0c => v0.0.0-20220624220833-87e55d714810
- `google.golang.org/api`: v0.84.0 => v0.85.0
- `google.golang.org/genproto`: v0.0.0-20220617124728-180714bec0ad => v0.0.0-20220624142145-8cd45d7dbd1f

## [v0.6.22](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.21...v0.6.22) (2022-06-20)

Upgraded Dependencies:

- `cloud.google.com/go`: v0.102.0 => v0.102.1
- `cloud.google.com/go/bigquery`: v1.32.0 => v1.33.0
- `cloud.google.com/go/compute`: v1.6.1 => v1.7.0
- `golang.org/x/net`: v0.0.0-20220607020251-c690dde0001d => v0.0.0-20220617184016-355a448f1bc9
- `golang.org/x/sys`: v0.0.0-20220520151302-bc2c85ada10a => v0.0.0-20220615213510-4f61da869c0c
- `google.golang.org/api`: v0.82.0 => v0.84.0
- `google.golang.org/genproto`: v0.0.0-20220607140733-d738665f6195 => v0.0.0-20220617124728-180714bec0ad

Added Dependencies:

- `github.com/googleapis/enterprise-certificate-proxy`: v0.1.0

## [v0.6.21](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.20...v0.6.21) (2022-06-08)

Upgraded Dependencies:

 - `golang.org/x/net`: v0.0.0-20220526153639-5463443f8c37 => v0.0.0-20220607020251-c690dde0001d
 - `golang.org/x/sync`: v0.0.0-20220513210516-0976fa681c29 => v0.0.0-20220601150217-0de741cfad7f
 - `google.golang.org/api`: v0.81.0 => v0.82.0
 - `google.golang.org/genproto`: v0.0.0-20220527130721-00d5c0f3be58 => v0.0.0-20220607140733-d738665f6195
 - `google.golang.org/grpc`: v1.46.2 => v1.47.0
 
## [v0.6.20](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.19...v0.6.20) (2022-05-30)

Upgraded Dependencies:

- `cloud.google.com/go`: v0.101.1 => v0.102.0
- `golang.org/x/net`: v0.0.0-20220517181318-183a9ca12b87 => v0.0.0-20220526153639-5463443f8c37
- `golang.org/x/oauth2`: v0.0.0-20220411215720-9780585627b5 => v0.0.0-20220524215830-622c5d57e401
- `golang.org/x/sys`: v0.0.0-20220517195934-5e4e11fc645e => v0.0.0-20220520151302-bc2c85ada10a
- `google.golang.org/api`: v0.80.0 => v0.81.0
- `google.golang.org/genproto`: v0.0.0-20220505152158-f39f71e6c8f3 => v0.0.0-20220527130721-00d5c0f3be58

## [v0.6.19](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.18...v0.6.19) (2022-05-18)

Update Dependencies:

- update `google.golang.org/grpc` to v1.46.2 (was v1.46.2);
- update `google.golang.org/api` to v0.80.0 (was v0.78.0);
- update `github.com/googleapis/gax-go/v2` to v2.4.0 (was v2.3.0);

Updated Indirect Dependencies:

- update `golang.org/x/sys`, `golang.org/x/net`, `google.golang.org/genproto`
  and `golang.org/x/xerrors` to latest (no semver);

## [v0.6.18](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.17...v0.6.18) (2022-05-10)

Updated Dependencies:

- update `cloud.google.com/bigquery` to v1.32.0 (was v1.31.0);
- update `cloud.google.com/go` to v0.101.1 (was v0.101.0);
- update `google.golang.org/api` to v0.78.0 (was v0.77.0);

Updated Indirect Dependencies:

- update `golang.org/x/sys`, `golang.org/x/net`, and `google.golang.org/genproto` to latest (no semver);

## [v0.6.17](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.16...v0.6.17) (2022-05-02)

Updated Dependencies:

- update `google.golang.org/api` to v0.77.0 (was v0.75.0);

Updated Indirect Dependencies:

- update `golang.org/x/sys`, `golang.org/x/net`, and `google.golang.org/genproto` to latest (no semver);

Added Indirect Dependencies:

- add `github.com/google/go-cmp` v0.5.8


## [v0.6.16](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.15...v0.6.16) (2022-04-25)

Updated Dependencies:

- update `google.golang.org/grpc` to v1.46.0 (was v1.45.0);
- update `cloud.google.com/go` to v0.101.0 (was v0.100.2);
- update `google.golang.org/api` to v0.75.0 (was v0.74.0);

Updated Indirect Dependencies:

- update `cloud.google.com/go/compute` to v1.6.1 (was v1.6.0);
- update `golang.org/x/sys`, `golang.org/x/net`,
  `golang.org/x/oauth2` and `google.golang.org/genproto` to latest (no semver);

## [v0.6.15](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.14...v0.6.15) (2022-04-19)

Updated Dependencies:

- update `cloud.google.com/bigquery` to v1.31.0 (was v1.30.2);
- update `github.com/googleapis/gax-go/v2` to v2.3.0 (was v2.2.0);

Updated Indirect Dependencies:

- update `cloud.google.com/go/compute` to v1.6.0 (was v1.5.0);
- update `golang.org/x/sys`, `golang.org/x/net`,
  `golang.org/x/oauth2` and `google.golang.org/genproto` to latest (no semver);

## [v0.6.14](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.13...v0.6.14) (2022-04-11)

Updated Indirect Dependencies:

- update `golang.org/x/sys`, `golang.org/x/net` and `google.golang.org/genproto` to latest (no semver);

## [v0.6.13](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.12...v0.6.13) (2022-04-01)

Updated Dependencies:

- update `cloud.google.com/bigquery` to v1.30.2 (was v1.30.0);
- update `google.golang.org/api` to v0.74.0 (was v0.73.0);

Updated Indirect Dependencies:

- update `golang.org/x/sys` and `google.golang.org/genproto` to latest (no semver);

## [v0.6.12](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.11...v0.6.12) (2022-03-22)

Updated Dependencies:

- update `cloud.google.com/bigquery` to v1.30.0 (was v1.29.0);
- update `github.com/googleapis/gax-go/v2` to v2.2.0 (was v2.1.1);
- update `google.golang.org/api` to v0.73.0 (was v0.71.0);

Updated Indirect Dependencies:

- update `google.golang.org/protobuf` to v1.28.0 (was v1.27.0);
- update `golang.org/x/sys` and `google.golang.org/genproto` to latest (no semver);

## [v0.6.11](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.10...v0.6.11) (2022-03-14)

Updated Dependencies:

- update `google.golang.org/api` to v0.71.0 (was v0.70.0);
- update `cloud.google.com/bigquery` to v1.29.0 (was v1.28.0);
  - ease of use (suggested by @glendc): introduce `TableParentFromParts`
    in order to auto-format the parent table URI correctly;
- update `google.golang.org/grpc` to v1.45.0 (was v1.44.0);

Updated Indirect Dependencies:

- update `cloud.google.com/go/iam` to v0.3.0 (was v0.2.0);
- update `golang.org/x/oauth2`, `golang.org/x/sys`
  and `google.golang.org/genproto` to latest (no semver);

## [v0.6.10](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.9...v0.6.10) (2022-02-28)

Updated Dependencies:

- update `google.golang.org/api` to v0.70.0 (was v0.69.0);

Updated Indirect Dependencies:

- update `cloud.google.com/go/compute` to v1.5.0 (was v1.3.0);
- update `golang.org/x/net`, `golang.org/x/oauth`, `golang.org/x/sys`
  and `google.golang.org/genproto` to latest (no semver);

## [v0.6.9](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.8...v0.6.9) (2022-02-21)

Updated Dependencies:

- update `google.golang.org/api` to v0.69.0 (was v0.68.0);
- update `cloud.google.com/bigquery` to v1.28.0 (was v1.27.0);

Updated Indirect Dependencies:

- update `google.golang.org/genproto` to latest (no semver);

## [v0.6.8](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.7...v0.6.8) (2022-02-14)

Updated Dependencies:

- update `google.golang.org/api` to v0.68.0 (was v0.67.0);

Updated Indirect Dependencies:

- update `cloud.google.com/go/compute` to v1.3.0 (was v1.2.0);
- update `golang.org/x/sys` and `google.golang.org/genproto` to latest (no semver);

Added Indirect Dependencies:

- added `cloud.google.com/go/iam` at v0.2.0;

## [v0.6.7](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.6...v0.6.7) (2022-02-07)

Updated Dependencies:

- update `google.golang.org/api` to v0.67.0 (was v0.66.0);

Updated Indirect Dependencies:

- update `cloud.google.com/go/compute` to v1.2.0 (was v1.1.0);
- update `golang.org/x/sys` and `google.golang.org/genproto` to latest (no semver);

## [v0.6.6](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.5...v0.6.6) (2022-01-31)

Updated Dependencies:

- update `cloud.google.com/bigquery` to v1.27.0 (was v1.26.0);
  - augments internal retry logic for `insertAll` (default) client: <https://github.com/googleapis/google-cloud-go/pull/5387/files>;
- update `google.golang.org/grpc` to v1.44.0 (was v1.43.0);

Updated Indirect Dependencies:

- update `cloud.google.com/go/compute` to v1.1.0 (was v1.0.0);
- update `google.golang.org/api` to v0.66.0 (was v0.65.0);
- update `golang.org/x/net`, `golang.org/x/sys` and `google.golang.org/genproto` to latest (no semver);

Removed Indirect Dependencies:
- remove `cloud.google.com/go/iam`;

## [v0.6.5](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.4...v0.6.5) (2022-01-17)

Dependencies:

- update `cloud.google.com/bigquery` to v1.26.0 (was v1.25.0);
- update `cloud.google.com/go` to v0.100.2 (was v0.99.0);
- update `cloud.google.com/api` to v0.65.0 (was v0.63.0);

Indirect Dependencies:

- update `github.com/cncf/xds`, `github.com/cncf/xds/go`, `github.com/udpa/go`,
  `golang.org/x/net`, `golang.org/x/sys` and `google.golang.org/genproto`
  to latest version (no semver);

New Indirect Dependencies:
- add `cloud.google.com/go/compute` (v1.0.0);
- add `cloud.google.com/go/iam` (v0.1.0);
- add `golang.org/x/text` (v0.3.7);

Removed Indirect Dependencies:
- remove `github.com/census-instrumentation/opencensus-proto`;
- remove `github.com/cespare/xxhash/v2`;
- remove `github.com/davecgh/go-spew`;
- remove `github.com/envoyproxy/go-control-plane`;
- remove `github.com/envoyproxy/protoc-gen-validate`;

## [v0.6.4](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.3...v0.6.4) (2022-01-04)

Dependencies:

- update `cloud.google.com/go` to v0.99.0 (was v0.98.0);
- update `cloud.google.com/api` to v0.63.0 (was v0.61.0);
- update `cloud.google.com/grpc` to v1.43.0 (was v1.42.0);
- update `github.com/cncf/xds`, `github.com/cncf/xds/go`, `golang.org/x/net`, `golang.org/x/sys` and `google.golang.org/genproto`
  to latest version (no semver);

## [v0.6.3](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.2...v0.6.3) (2021-12-06)

Maintenance:

- reduce cyclomatic complexities in test/integration/main.go;
- add comments to internal functions for which it was missing;

Dependencies:

- update `cloud.google.com/go/bigquery` to v1.25.0 (was v1.24.0):
  - contained a breaking change in the ManagedWriter API, `NoStreamOffset` is no longer to be passed
    to `AppendRow` and instead the call is to be made without an option for our purposes (Default streams);
- update `cloud.google.com/go` to v0.98.0 (was v0.97.0);
- update `cloud.google.com/api` to v0.61.0 (was v0.60.0);
- update `github.com/cncf/xds/go`, `golang.org/x/net`, `golang.org/x/sys` and `google.golang.org/genproto`
  to latest version (no semver);

Documentation:

- Fix version code compare links in CHANGELOG.md;
- Add Awesome-Go badge to README as BQWriter is since 2021-11-17 listed in the awesome-go curated list;

## [v0.6.2](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.1...v0.6.2) (2021-11-17)

Documentation:

- Add a FAQ to the README.md documentation;

Testing:

- Improve test code coverage (was ~69%, now it is %80+);

Other changes:

- update `golang.org/x/net`, `golang.org/x/sys` and `google.golang.org/genproto`
  to latest version (no semver);

## [v0.6.1](https://www.github.com/OTA-Insight/bqwriter/compare/v0.6.0...v0.6.1) (2021-11-15)

Bug Fixes:

- Remove duplicate deadline for InsertAll client, this setting is no longer required as anyhow the
  insertAll inner BQ client has a max deadline which cannot be configured, which we can use as-is;
- Add Retry logic to InsertAll client as to support Retrying tmp/internal BQ errors,
  these are by design not supported by BigQuery google Go API itself,
  as can be read on https://github.com/googleapis/google-cloud-go/issues/3792,
  but we do want to support these as retryable as that is usually what you want to do;
  - Permanent issues will fail after ~32s and will non the less end up in the user's logs;
- Reset flush ticket in Streamer always, even in case of a put-flush error, this is fine by convention;

Documentation:

- Add a CONTRIBUTORS file to list anyone who contributed to this project, and is not listed as an AUTHOR;

Other changes:

- Change DefaultMaxBatchDelay from 5s to 10s;
- update `golang.org/x/net`, `golang.org/x/sys` and `google.golang.org/genproto`
  to latest version (no semver);

## [v0.6.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.5.1...v0.6.0) (2021-11-12)

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

## [v0.5.1](https://www.github.com/OTA-Insight/bqwriter/compare/v0.5.0...v0.5.1) (2021-11-10)

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

## [v0.5.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.4.1...v0.5.0) (2021-11-09)

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

## [v0.4.1](https://www.github.com/OTA-Insight/bqwriter/compare/v0.4.1...v0.4.0) (2021-12-06)

- add storage API driven stream examples to README;
- fix nil deref bug when creating streamer using StorageAPI;

## [v0.4.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.4.0...v0.3.1) (2021-11-05)

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

## [v0.3.1](https://www.github.com/OTA-Insight/bqwriter/compare/v0.3.1...v0.3.0) (2021-10-28)

- fix README.md badges and rename LICENSE to LICENSE.txt;
- updated dependencies to latest:
  - golang.org/x/*: 2021-10-20+;
  - google.golang.org/api: v0.59.0;

## [v0.3.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.3.0...v0.2.0) (2021-10-25)

- remove unused WriteRetryConfig (its use was eliminated in v0.2.0);
- fix a linter issue found in `v0.2.0`'s `streamer.go` codebase (indention);
- add golint-ci dev + CI support for better code quality;

## [v0.2.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.2.0...v0.1.0) (2021-10-19)

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
