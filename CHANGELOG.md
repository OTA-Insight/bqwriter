# Changes

## [v0.4.0](https://www.github.com/OTA-Insight/bqwriter/compare/v0.3.1...v0.4.0) (ETA: 2021-11-05)

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
