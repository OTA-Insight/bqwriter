# Changes

## v0.3.0 (2021-10-20)

- remove unused WriteRetryConfig (its use was eliminated in v0.2.0);
- fix a linter issue found in v0.2.0 `streamer.go` codebase (indention);

work in progress

## v0.2.0 (2021-10-19)

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
