# Gnat

A [nats.io](https://nats.io/) client for elixir.
The goals of the project are resiliency, performance, and ease of use.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `gnat` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:gnat, "~> 0.1.0"}]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/gnat](https://hexdocs.pm/gnat).

## Benchmarks

Part of the motivation for building this library is to get better performance.
To this end I've started a `bench.exs` script that we can use to check our performance.
As of this commit the most recent numbers from running on my macbook pro are:

|   | ips | average | deviation | median |
| - | --- | ------- | --------- | ------ |
| parse-128 | 90.54 K | 11.04 μs | ±78.18% | 10.00 μs |
| pub - 128 | 88.03 K | 11.36 μs | ±82.08% | 11.00 μs |
| subpub-16 | 7.44 K | 134.49 μs | ±47.57% | 122.00 μs |
