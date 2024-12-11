[![hex.pm](https://img.shields.io/hexpm/v/gnat.svg)](https://hex.pm/packages/gnat)
[![hex.pm](https://img.shields.io/hexpm/dt/gnat.svg)](https://hex.pm/packages/gnat)
[![hex.pm](https://img.shields.io/hexpm/l/gnat.svg)](https://hex.pm/packages/gnat)
[![github.com](https://img.shields.io/github/last-commit/nats-io/nats.ex.svg)](https://github.com/nats-io/nats.ex)

![NATS](https://nats.io/img/logos/nats-horizontal-color.png)

# Gnat

A [nats.io](https://nats.io/) client for Elixir.
The goals of the project are resiliency, performance, and ease of use.

> Hex documentation available here: https://hex.pm/packages/gnat

## Usage

``` elixir
{:ok, gnat} = Gnat.start_link(%{host: "127.0.0.1", port: 4222})
# Or if the server requires TLS you can start a connection with:
# {:ok, gnat} = Gnat.start_link(%{host: "127.0.0.1", port: 4222, tls: true})

{:ok, subscription} = Gnat.sub(gnat, self(), "pawnee.*")
:ok = Gnat.pub(gnat, "pawnee.news", "Leslie Knope recalled from city council (Jammed)")
receive do
  {:msg, %{body: body, topic: "pawnee.news", reply_to: nil}} ->
    IO.puts(body)
end
```

## Authentication

``` elixir
# with user and password
{:ok, gnat} = Gnat.start_link(%{host: "127.0.0.1", port: 4222, username: "joe", password: "123", auth_required: true})

# with token
{:ok, gnat} = Gnat.start_link(%{host: "127.0.0.1", port: 4222, token: "secret", auth_required: true})

# with an nkey seed
{:ok, gnat} = Gnat.start_link(%{host: "127.0.0.1", port: 4222, nkey_seed: "SUAM...", auth_required: true})

# with decentralized user credentials (JWT)
{:ok, gnat} = Gnat.start_link(%{host: "127.0.0.1", port: 4222, nkey_seed: "SUAM...", jwt: "eyJ0eX...", auth_required: true})

# connect to NGS with JWT
{:ok, gnat} = Gnat.start_link(%{host: "connect.ngs.global", tls: true, jwt: "ey...", nkey_seed: "SUAM..."})
```

## TLS Connections

[NATS Server](https://github.com/nats-io/nats-server) is often configured to accept or require TLS connections.
In order to connect to these clusters you"ll want to pass some extra TLS settings to your `Gnat` connection.

``` elixir
# using a basic TLS connection
{:ok, gnat} = Gnat.start_link(%{host: "127.0.0.1", port: 4222, tls: true})

# Passing a Client Certificate for verification
{:ok, gnat} = Gnat.start_link(%{tls: true, ssl_opts: [certfile: "client-cert.pem", keyfile: "client-key.pem"]})
```

## Resiliency

If you would like to stay connected to a cluster of nats servers, you should consider using `Gnat.ConnectionSupervisor` .
This can be added to your supervision tree in your project and will handle automatically re-connecting to the cluster.

For long-lived subscriptions consider using `Gnat.ConsumerSupervisor` .
This can also be added to your supervision tree and use a supervised connection to re-establish a subscription.
It also handles details like handling each message in a supervised process so you isolate failures and get OTP logs when an unexpected error occurs.

## Services
If you supply a module that implements the `Gnat.Services.Server` behavior and the `service_definition` 
configuration field to a `Gnat.ConsumerSupervisor`, then this client will automatically take care
of exposing the service to discovery, responding to pings, and maintaining and exposing statistics like request and error counts, and processing times.

## Instrumentation

Gnat uses [telemetry](https://hex.pm/packages/telemetry) to make instrumentation data available to clients.
If you want to record metrics around the number of messages or latency of message publishes, subscribes, requests, etc you can do the following in your project:

``` elixir
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
  [:gnat, :unsub],
  [:gnat, :service_request],
  [:gnat, :service_error]
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

The `pub` , `sub` , `request` , and `unsub` events all report the latency of those respective calls.
The `message_received` event reports a number of messages like `%{count: 1}` because there isn"t a good latency metric to report. Any microservices managed by a consumer supervisor will also report `service_request` and `service_error`. In addition to the `:topic` metadata, microservices will also include `:endpoint` and `:group` (which can be `nil`) in their telemetry reports.

All of the events (except `unsub` ) include metadata with a `:topic` key so you can split your metrics by topic.

## Benchmarks

Part of the motivation for building this library is to get better performance.
To this end, there is a `bench` branch on this project which includes a `server.exs` and `client.exs` that can be used for benchmarking various scenarios.

As of this commit, the [latest benchmark on a 16-core server](https://gist.github.com/mmmries/08fe44fdd47a6f8838936f41170f270a) shows that you can make 170k+ req/sec or up to 192MB/sec.

The `bench/*.exs` files also contain some straight-line single-CPU performance tests.
As of this commit my 2018 MacBook pro shows.

|               | ips      | average   | deviation | median |
| ------------- | -------- | --------- | --------- | ------ |
| parse-128     | 487.67 K | 2.19 μs   | ±1701.54% | 2 μs   |
| pub - 128     | 96.37 K  | 10.38 μs  | ±102.94%  | 10 μs  |
| req-reply-128 | 8.32 K   | 120.16 μs | ±23.68%   | 114 μs |

## Development

Before running the tests make sure you have a locally running copy of `nats-server` ([installation instructions](https://docs.nats.io/running-a-nats-service/introduction/installation)).

We currently use version `2.6.6` in CI, but anything higher than `2.2.0` should be fine.
Versions from `0.9.6` up to `2.2.0` should work fine for everything except header support.
Make sure to enable jetstream with the `nats-server -js` argument and you might also want to enable debug and verbose logging if you"re trying to understand the messages being sent to/from nats (ie `nats-server -js -D -V`).
The typical `mix test` will run all the basic unit tests.

You can also run the `multi_server` set of tests that test connectivity to different
`nats-server` configurations. You can run these with `mix test --only multi_server` .
The tests will tell you how to start the different configurations.

There are also some property-based tests that generate a lot of test cases.
You can tune how many test cases by setting the environment variable `N=200 mix test --only property` (default it 100).

For more details you can look at how Travis runs these things in the CI flow.
