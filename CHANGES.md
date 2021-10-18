# Changes

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
