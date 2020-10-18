# Changelog

## 1.2

* `Gnat.Server` behaviour with support in the `ConsumerSupervisor` https://github.com/nats-io/nats.ex/compare/1b1adc85e4b28231218ef87c7fc3445fce854377...b24a7e14325b51fbb93fde7e3d891d18b4fa8afb
* avoid logging sensitive credentials https://github.com/nats-io/nats.ex/pull/105
* deprecate Gnat.ping, improved typespecs https://github.com/nats-io/nats.ex/pull/103

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
