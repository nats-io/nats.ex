# Using Broadway with Jetstream

Broadway is a library which allows building concurrent and multi-stage data ingestion and data
processing pipelines with Elixir easily. You can learn about it more in
[Broadway documentation](https://hexdocs.pm/broadway/introduction.html).

Jetstream library comes with tools necessary to use NATS Jetstream with Broadway.

## Getting started

In order to use Broadway with NATS Jetstream you need to:

1. Setup a NATS Server with JetStream turned on
2. Create stream and consumer on NATS server
3. Configure Gnat connection in your Elixir project
4. Configure your project to use Broadway

In this guide, we are going to focus on the fourth point. To learn how to start Jetstream locally
with Docker Compose and then add Gnat and Jetstream to your application, see the Starting Jetstream
section in [Getting Started guide](../introduction/getting_started.md).

### Adding Broadway to your application

Once we have NATS with JetStream running and the stream and consumer we are going to use are
created, we can proceed to adding Broadway to our project. First, put `:broadway` to the list of
dependencies in `mix.exs`.

```elixir
defp deps do
  [
    ...
    {:broadway, ...version...},
    ...
  ]
end
```

Visit [Broadway page on Hex.pm](https://hex.pm/packages/broadway) to check for current version
to put in `deps`.

To install the dependencies, run:

```shell
mix deps.get
```

### Defining the pipeline configuration

The next step is to define your Broadway module. We need to implement three functions in order
to define a Broadway pipeline: `start_link/1`, `handle_message/3` and `handle_batch/4`.
Let's create `start_link/1` first:

```elixir
defmodule MyBroadway do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(
      __MODULE__,
      name: MyBroadwayExample,
      producer: [
        module: {
          OffBroadway.Jetstream.Producer,
          connection_name: :gnat,
          stream_name: "TEST_STREAM",
          consumer_name: "TEST_CONSUMER"
        },
        concurrency: 10
      ],
      processors: [
        default: [concurrency: 10]
      ],
      batchers: [
        default: [
          concurrency: 5,
          batch_size: 10,
          batch_timeout: 2_000
        ]
      ]
      ...
    )
  end

  ...callbacks..
end
```

All `start_link/1` does is just delegating to `Broadway.start_link/2`.

To understand what all these options mean and to learn about other possible settings, visit
[Broadway documentation](https://hexdocs.pm/broadway/Broadway.html).

The part that interests us the most in this guide is the `producer.module`. Here we're choosing
`OffBroadway.Jetstream.Producer` as the producer module and passing the connection options,
such as Gnat process name and stream name. For full list of available options, visit
[Producer](`OffBroadway.Jetstream.Producer`) documentation.

### Implementing Broadway callbacks

Broadway requires some callbacks to be implemented in order to process messages. For full list
of available callbacks visit
[Broadway documentation](https://hexdocs.pm/broadway/Broadway.html#callbacks).

A simple example:

```elixir
defmodule MyBroadway do
  use Broadway

  alias Broadway.Message

  ...start_link...

  def handle_message(_processor_name, message, _context) do
    message
    |> Message.update_data(&process_data/1)
    |> case do
      "FOO" -> Message.configure_ack(on_success: :term)
      "BAR" -> Message.configure_ack(on_success: :nack)
      message -> message
    end
  end

  defp process_data(data) do
    String.upcase(data)
  end

  def handle_batch(_, messages, _, _) do
    list = messages |> Enum.map(fn e -> e.data end)
    IO.puts("Got a batch: #{inspect(list)}. Sending acknowledgements...")
    messages
  end
```

First, in `handle_message/3` we update our messages' data individually by converting them to
uppercase. Then, in the same callback, we're changing the success ack option of the message
to `:term` if its content is `"FOO"` or to `:nack` if the message is `"BAR"`. In the end we
print each batch in `handle_batch/4`. It's not quite useful but should be enough for this
guide.

## Running the Broadway pipeline

Once we have our pipeline fully defined, we need to add it as a child in the supervision tree.
Most applications have a supervision tree defined at `lib/my_app/application.ex`.

```elixir
children = [
  {MyBroadway, []}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

You can now test the pipeline. Let's start the application:

```shell
iex -S mix
```

Use Gnat API to send messages to your stream:

```elixir
Gnat.pub(:gnat, "test_subject", "foo")
Gnat.pub(:gnat, "test_subject", "bar")
Gnat.pub(:gnat, "test_subject", "baz")
```

Batcher should then print:

```
Got a batch: ["FOO", "BAR", "BAZ"]. Sending acknowledgements...
```