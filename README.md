[![CircleCI](https://circleci.com/gh/nats-io/nats.ex/tree/master.svg?style=svg)](https://circleci.com/gh/nats-io/nats.ex/tree/master)

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
iex(1)> metrics_function = fn(event_name, measurements, event_meta, config) ->
  IO.inspect([event_name, measurements, event_meta, config])
  :ok
end
#Function<4.128620087/4 in :erl_eval.expr/5>
iex(2)> names = [[:gnat, :pub], [:gnat, :sub], [:gnat, :message_received], [:gnat, :request], [:gnat, :unsub]]
[
  [:gnat, :pub],
  [:gnat, :sub],
  [:gnat, :message_received],
  [:gnat, :request],
  [:gnat, :unsub]
]
iex(3)> :telemetry.attach_many("my listener", names, metrics_function, %{my_config: true})
:ok
iex(4)> {:ok, gnat} = Gnat.start_link()
{:ok, #PID<0.203.0>}
iex(5)> Gnat.sub(gnat, self(), "topic")
[[:gnat, :sub], %{latency: 128000}, %{topic: "topic"}, %{my_config: true}]
{:ok, 1}
iex(6)> Gnat.pub(gnat, "topic", "ohai")
[[:gnat, :pub], %{latency: 117000}, %{topic: "topic"}, %{my_config: true}]
[[:gnat, :message_received], %{count: 1}, %{topic: "topic"}, %{my_config: true}]
:ok
```

The `pub`, `sub`, `request`, and `unsub` events all report the latency of those respective calls.
The `message_received` event reports a number of messages like `%{count: 1}` because there isn't a good latency metric to report.
All of the events (except `unsub`) include metadata with a `:topic` key so you can split your metrics by topic.

## Benchmarks

Part of the motivation for building this library is to get better performance.
To this end, there is a `bench` branch on this project which includes a `server.exs` and `client.exs` that can be used for benchmarking various scenarios.

As of this commit, the [latest benchmark on a 16-core server](https://gist.github.com/mmmries/08fe44fdd47a6f8838936f41170f270a) shows that you can make 170k+ req/sec or up to 192MB/sec.

The `bench/*.exs` files also contain some straight-line single-cpu performance tests.
As of this commit my 2018 macbook pro shows.

|   | ips | average | deviation | median |
| - | --- | ------- | --------- | ------ |
| parse-128 | 487.67 K | 2.19 μs | ±1701.54% | 2 μs |
| pub - 128 | 96.37 K | 10.38 μs | ±102.94% | 10 μs |
| req-reply-128 | 8.32 K | 120.16 μs | ±23.68% | 114 μs |

## Development

Before running the tests make sure you have a locally running copy of `gnatsd` (`brew install gnatsd`).
We currently use version `1.4.1` in CI, but anything higher than `0.9.6` should be fine.
The typical `mix test` will run all the basic unit tests.

You can also run the `multi_server` set of tests that test connectivity to different
`gnatsd` configurations. You can run these with `mix test --only multi_server`.
The tests will tell you how to start the different configurations.

There are also some property-based tests that generate a lot of test cases.
You can tune how many test cases by setting the environment variable `N=200 mix test --only property` (default it 100).

For more details you can look at how Travis runs these things in the CI flow.
