# Changelog

## 1.10

* Clarify authentication setup during test by @davydog187 in #187
* Test on Elixir 1.18 and NATS 2.10.24 by @davydog187 in #188
* Gnat.Jetstream.API.KV.info/3 by @davydog187 in #189
* Remove function_exported? check for Keyword.validate!/2 by @davydog187 in #186
* Tiny optimization to KV.list_buckets/1 by @davydog187 in #185
* make KV-watcher emit :key_added events when the message has a header by @rixmann in #191
* add :compression to stream attributes by @rixmann in #192
* fix: unknown field domain in Stream.create (#194) by @c0deaddict
* feat: add jetstream message metadata helper (#197) by @c0deaddict
* fix: deliver policy (#196) by @c0deaddict

## 1.9

* Housecleaning by @mmmries in #176
  * switch to charlist sigils
  * update to newest nkeys
  * require elixir 1.14 and erlang 25+
* Fix incorrect useage of charlist by @davydog187 in #179
* Soft deprecate is_kv_bucket_stream?/1 in favor of kv_bucket_stream?/1 by @davydog187 in #183
* Clean up examples in KV by @davydog187 in #181
* Document options for Gnat.Jetstream.API.KV by @davydog187 in #180

## 1.8

* Integrated the jetstream functionality into this client directly https://github.com/nats-io/nats.ex/pull/146
* Add ability to list KV buckets https://github.com/nats-io/nats.ex/pull/152
* Improve CI Reliability https://github.com/nats-io/nats.ex/pull/154
* Bugfix to treat no streams as an empty list rather than a null https://github.com/nats-io/nats.ex/pull/155
* Added supported for `allow_direct` and `mirror_direct` attributes of streams https://github.com/nats-io/nats.ex/pull/161
* Added support for `discard_new_per_subject` attribute of streams https://github.com/nats-io/nats.ex/pull/163
* Added support for `Object.list_buckets` https://github.com/nats-io/nats.ex/pull/169

## 1.7

 * Added support for the NATS [services API](https://github.com/nats-io/nats.go/blob/main/micro/README.md), letting developers participate in service discovery and stats https://github.com/nats-io/nats.ex/pull/141
 * A bugfix to remove the queue_group from a service config and some optimization for the services API https://github.com/nats-io/nats.ex/pull/145

## 1.6

* added the `no_responders` behavior https://github.com/nats-io/nats.ex/pull/137

## 1.5

* add the `inbox_prefix` option https://github.com/nats-io/nats.ex/pull/121
* add the `Gnat.server_info/1` function https://github.com/nats-io/nats.ex/pull/124
* fix header parsing issue https://github.com/nats-io/nats.ex/pull/125

## 1.4

* add the `Gnat.request_multi/4` function https://github.com/nats-io/nats.ex/pull/120
* add elixir 1.13 to the test matrix

## 1.3

* adding support for sending and receiving headers https://github.com/nats-io/nats.ex/pull/116

## 1.2

* `Gnat.Server` behaviour with support in the `ConsumerSupervisor` https://github.com/nats-io/nats.ex/compare/1b1adc85e4b28231218ef87c7fc3445fce854377...b24a7e14325b51fbb93fde7e3d891d18b4fa8afb
* avoid logging sensitive credentials https://github.com/nats-io/nats.ex/pull/105
* deprecate Gnat.ping, improved typespecs https://github.com/nats-io/nats.ex/pull/103 
* relax the version constraint on nimble_parsec https://github.com/nats-io/nats.ex/issues/112

## 1.1

* add support for nkeys and NGS https://github.com/nats-io/nats.ex/pull/101
* Fix supervisor ConsumerSuperivsor crash https://github.com/nats-io/nats.ex/pull/96

## 1.0

* Make supervisors officially supported https://github.com/nats-io/nats.ex/pull/96

## 0.7.0

* update to telemetry 0.4 https://github.com/nats-io/nats.ex/pull/86 and https://github.com/nats-io/nats.ex/pull/87
* support for token authentication https://github.com/nats-io/nats.ex/pull/92
* support elixir 1.9 https://github.com/nats-io/nats.ex/pull/93

## 0.6.0

* Dropped support for Erlang < 19 and Elixir <= 1.5
* Added Telemetry to the project (thanks @rubysolo)
* Switched to nimble_parsec for parsing
  * Updated benchmarking/performance information. We can now do 170k requests per second on a 16-core server.
* Fixed a bug around re-subscribing for the `ConsumerSupervisor`
* Pass `sid` when delivering message (thanks @entone)
* Documentation fixes from @deini and @johannestroeger

## 0.5.0

* Dropped support for Elixir 1.4 and OTP 18 releases. You will need to use Elixir 1.5+ and OTP 19+.
* Switched to running our tests against gnatsd `1.3.0`
