[![Build Status](https://travis-ci.org/mmmries/gnat.svg?branch=master)](https://travis-ci.org/mmmries/gnat)

![gnat](gnat.png)

# Gnat

A [nats.io](https://nats.io/) client for elixir.
The goals of the project are resiliency, performance, and ease of use.

## Usage

```elixir
{:ok, gnat} = Gnat.start_link(%{host: '127.0.0.1', port: 4222})
# Or if the server requires TLS you can start a connection with:
# {:ok, gnat} = Gnat.start_link(%{host: '127.0.0.1', port: 4222, tls: true})

{:ok, subscription} = Gnat.sub(gnat, self(), "pawnee.*")
:ok = Gnat.pub(gnat, "pawnee.news", "Leslie Knope recalled from city council (Jammed)")
receive do
  {:msg, %{body: body, topic: "pawnee.news", reply_to: nil}} ->
    IO.puts(body)
end
```

## Instrumentation

Gnat uses [telemetry](https://hex.pm/packages/telemetry) to make instrumentation data available to clients.
If you want to record metrics around the number of messages or latency of message publishes, subscribes, requests, etc you can do the following in your project:

```elixir
Telemetry.attach("record_pubs", [:gnat, :pub], YourMetricRecorder, :publish, nil)
Telemetry.attach("record_subs", [:gnat, :sub], YourMetricRecorder, :subscribe, nil)
Telemetry.attach("record_requests", [:gnat, :request], YourMetricRecord, :request, nil)
Telemetry.attach("record_messages", [:gnat, :message_received], YourMetricRecorder, :message_received, nil)
Telemetry.attach("record_unsubs", [:gnat, :unsub], YourMetricRecorder, :unsub, nil)
```

The `pub`, `sub`, `request`, and `unsub` events all report the latency of those respective calls.
The `message_received` event always reports a value of `1` because there isn't a good latency metric to report.
All of the events include metadata with a `topic` key so you can split your metrics by topic (the `unsub` metric is the only one that doesn't have a `topic` available).

## Benchmarks

Part of the motivation for building this library is to get better performance.
To this end, there is a `bench` branch on this project which includes a `server.exs` and `client.exs` that can be used for benchmarking various scenarios.

As of this commit, the [latest benchmark on a 16-core server](https://gist.github.com/mmmries/08fe44fdd47a6f8838936f41170f270a) shows that you can make 70k+ requests per second.

The `bench.exs` file also contains some straight-line performance tests.
As of this commit my 2014 macbook pro shows.

|   | ips | average | deviation | median |
| - | --- | ------- | --------- | ------ |
| parse-128 | 77.14 K | 12.96 μs | ±161.76% | 12.00 μs |
| pub - 128 | 37.22 K | 26.87 μs | ±62.84% | 24.00 μs |
| sub-unsub-pub16 | 5.03 K | 198.68 μs | ±37.81% | 184.00 μs |
| req-reply-4 | 2.98 K | 335.26 μs | ±18.04% | 326.00 μs |

## Development

Before running the tests make sure you have a locally running copy of `gnatsd` (`brew install gnatsd`).
We currently use version `1.3.0` in CI, but anything higher than `0.9.6` should be fine.
The typical `mix test` will run all the basic unit tests.

You can also run the `multi_server` set of tests that test connectivity to different
`gnatsd` configurations. You can run these with `mix test --only multi_server`.
The tests will tell you how to start the different configurations.

There are also some property-based tests that generate a lot of test cases.
You can tune how many test cases by setting the environment variable `N=200 mix test --only property` (default it 100).

For more details you can look at how Travis runs these things in the CI flow.
