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

## Benchmarks

Part of the motivation for building this library is to get better performance.
To this end I've started a `bench.exs` script that we can use to check our performance.
As of this commit the most recent numbers from running on my macbook pro are:

|   | ips | average | deviation | median |
| - | --- | ------- | --------- | ------ |
| parse-128 | 81.86 K | 12.22 μs | ±177.12% | 11.00 μs |
| pub - 128 | 146.22 K | 6.84 μs | ±450.03% | 6.00 μs |
| sub-unsub-pub16 | 9.06 K | 110.37 μs | ±68.61% | 102.00 μs |
| req-reply-4 | 5.67 K | 176.45 μs | ±19.81% | 165.00 μs |

These benchmarks all show single-actor performance with a locally running gnats server.
Running 32 client actors on an 8-core ubuntu server sending requests to another 8-core ubuntu server running 2 gnat subscriber actors we achieved:
* 19,920 requests/sec
* 90th % latency of 2.2ms

[see details in the performance issue](https://github.com/mmmries/gnat/issues/28)

## Development

To run the tests the typical `mix test` will run all tests that have not been tagged
to exclude as shown in the `test_helper.exs` file.  There are some tests that require
another server to be running.  These are marked with `@tag :multi_server` and can be
included in a test run with the following command `mix test --include multi_server`
