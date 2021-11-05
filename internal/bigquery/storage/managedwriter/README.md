# BQ Storage: ManagedWriter

The code in this package is a slightly modified fork from
<https://github.com/googleapis/google-cloud-go/tree/a2af4de215a42848368ec3081263d34782032caa/bigquery/storage/managedwriter>.

NOTE: eventually we want to phase out our own managed writer fork,
here is what we definitely require from that package in order to do so:

- be able to define a custom Retryer for all its GRPC purposes,
  currently it is not possible for any purpose at all;
  - an issue (feature-request) is created for this: <https://github.com/googleapis/google-cloud-go/issues/5094>
- receive some kind of commitment from them that they do seriously consider,
  making this a production-ready part of the bigquery storage API;
- make the stats/diagnostic code opt-in;
