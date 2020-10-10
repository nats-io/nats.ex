# Changelog

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
