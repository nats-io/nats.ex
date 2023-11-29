# Getting Started

In this guide, we're going to learn how to install Jetstream in your project and start consuming
messages from your streams.

## Starting Jetstream

The following Docker Compose file will do the job:

```yaml
version: "3"
services:
  nats:
    image: nats:latest
    command:
      - -js
    ports:
      - 4222:4222
```

Save this snippet as `docker-compose.yml` and run the following command:

```shell
docker compose up -d
```

Let's also create Jetstream stream where we will publish our hello world messages:

```shell
nats stream add HELLO --subjects="greetings"
```

> #### Tip {: .tip}
>
> You can also manage Jetstream streams and consumers via Elixir. You can see more details in
> [this guide](../guides/managing.md).

## Adding Jetstream and Gnat to an application

To start off with, we'll generate a new Elixir application by running this command:

```
mix new hello_jetstream --sup
```

We need to have [a supervision tree](http://elixir-lang.org/getting-started/mix-otp/supervisor-and-application.html)
up and running in your app, and the `--sup` option ensures that.

To add Jetstream to this application, you need to add [Jetstream](https://hex.pm/packages/jetstream)
and [Gnat](https://hex.pm/packages/gnat) libraries to your `deps` definition in our `mix.exs` file.
**Fill exact version requirements from each package Hex.pm pages.**

```elixir
defp deps do
  [
    {:gnat, ...},
    {:jetstream, ...}
  ]
end
```

To install these dependencies, we will run this command:

```shell
mix deps.get
```

Now let's connect to our NATS server. To do this, you need to start `Gnat.ConnectionSupervisor`
under our application's supervision tree. Add following to `lib/hello_jetstream/application.ex`:

```elixir
def start(_type, _args) do
  children = [
    ...

    # Create NATS connection
    {Gnat.ConnectionSupervisor,
      %{
        name: :gnat,
        connection_settings: [
          %{host: "localhost", port: 4222}
        ]
      }},
  ]

  ...
```

This piece of configuration will start Gnat processes that connect to the NATS server and allow
publishing and subscribing to any subjects. Jetstream operates using plain NATS subjects which
follow specific naming and message format conventions.

Let's now create a _pull consumer_ which will subscribe a specific Jetstream stream and print
incoming messages to standard output.

## Creating a pull consumer

Jetstream requires us to allocate a view/cursor of the stream that our consumer will operate on.
In Jetstream terminology, this view is called a _consumer_ (Funnily enough we've just implemented
a consumer in our code, coincidence?). [Jetstream](https://docs.nats.io/nats-concepts/jetstream/consumers)
[documentation](https://docs.nats.io/nats-concepts/jetstream/consumers/example_configuration)
offers great insights on benefits of having this separate concept so we won't duplicate work here.

Jetstream offers two stream consuming modes: _push_ and _pull_.

In _push_ mode, Jetstream will simply send messages to selected consumers immediately when they are
received. This approach does offer congestion control, so it is not recommended for high-volume
and/or reliability sensitive streams. You do not really need this library to implement push
consumer because all building blocks are in `Gnat` library. You can read more about push consumers
in [this guide](../guides/push_based_consumer.md).

On the other hand, in _pull_ mode consumers ask Jetstream for more messages when they are ready
to process them. This is the recommended approach for most use cases and we will proceed with it
in this guide.

> #### This is just a brief outline {: .tip}
>
> For more details about differences between consumer modes, consult
> [Jetstream documentation](https://docs.nats.io/nats-concepts/jetstream/consumers).

Let's create a pull consumer module within our application at
`lib/hello_jetstream/logger_pull_consumer.ex`:

```elixir
defmodule HelloJetstream.LoggerPullConsumer do
  use Jetstream.PullConsumer

  def start_link([]) do
    Jetstream.PullConsumer.start_link(__MODULE__, [])
  end

  @impl true
  def init([]) do
    {:ok, nil, connection_name: :gnat, stream_name: "HELLO", consumer_name: "LOGGER"}
  end

  @impl true
  def handle_message(message, state) do
    IO.inspect(message)
    {:ack, state}
  end
end
```

Pull Consumer is a regular `GenServer` and it takes a reference to `Gnat.ConnectionSupervisor`
along with names of Jetstream stream and consumer as options passed to
`Jetstream.PullConsumer.start*` functions. These options are passed as keyword list in third element
of tuple returned from the `c:Jetstream.PullConsumer.init/1` callback.

The only required callbacks are well known gen server's `c:Jetstream.PullConsumer.init/1` and
`c:Jetstream.PullConsumer.handle_message/2`, which takes new message as its first argument and
is expected to return an _ACK action_ instructing underlying process loop what to do with this
message. Here we are asking it to automatically send for us an ACK message back to Jetstream.

Let's now create a consumer in our NATS server. We will call it `LOGGER` as we plan to let it simply
log everything published to the stream.

```shell
nats consumer add --pull --deliver=all HELLO LOGGER
```

Now, let's start our pull consumer under application's supervision tree.

```elixir
def start(_type, _args) do
  children = [
    ...

    # Jetstream Pull Consumer
    HelloJetstream.LoggerPullConsumer,
  ]

  ...
```

Let's now publish some messages to our `HELLO` stream, so something will be waiting for our
application to be read when it starts.

## Publishing messages to streams

Jetstream listens on regular NATS subjects, so publishing messages is dead simple with `Gnat.pub/3`:

```elixir
Gnat.pub(:gnat, "greetings", "Hello World")
```

Or via NATS CLI:

```shell
nats pub greetings "Hello World"
```

That's it! When you run your app, you should see your messages being read by your application.