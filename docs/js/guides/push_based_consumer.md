# Push based consumer

```elixir
# Start a nats server with jetstream enabled and default configs
# Now run the following snippets in an IEx terminal
alias Jetstream.API.{Consumer,Stream}

# Setup a connection to the nats server and create the stream/consumer
# This is the equivalent of these two nats cli commands
#   nats stream add TEST --subjects="greetings" --max-msgs=-1 --max-msg-size=-1 --max-bytes=-1 --max-age=-1 --storage=file --retention=limits --discard=old
#   nats consumer add TEST TEST --target consumer.greetings --replay instant --deliver=all --ack all --wait=5s --filter="" --max-deliver=10
{:ok, connection} = Gnat.start_link()
stream = %Stream{name: "TEST", subjects: ["greetings"]}
{:ok, _response} = Stream.create(connection, stream)
consumer = %Consumer{stream_name: "TEST", name: "TEST", deliver_subject: "consumer.greetings", ack_wait: 5_000_000_000, max_deliver: 10}
{:ok, _response} = Consumer.create(connection, consumer)

# Setup Consuming Function
defmodule Subscriber do
  def handle(msg) do
    IO.inspect(msg)
    case msg.body do
      "hola" -> Jetstream.ack(msg)
      "bom dia" -> Jetstream.nack(msg)
      _ -> nil
    end
  end
end

# normally you would add the `ConnectionSupervisor` and `ConsumerSupervisor` to your supervisrion tree
# here we start them up manually in an IEx session
{:ok, _pid} = Gnat.ConnectionSupervisor.start_link(%{
  name: :gnat,
  backoff_period: 4_000,
  connection_settings: [
    %{}
  ]
})
{:ok, _pid} = Gnat.ConsumerSupervisor.start_link(%{
  connection_name: :gnat,
  consuming_function: {Subscriber, :handle},
  subscription_topics: [
    %{topic: "consumer.greetings"}
  ]
})

# now publish some messages into the stream
Gnat.pub(:gnat, "greetings", "hello") # no ack will be sent back, so you'll see this message received 10 times with a 5sec pause between each one
Gnat.pub(:gnat, "greetings", "hola") # an ack is sent back so this will only be received once
Gnat.pub(:gnat, "greetings", "bom dia") # a -NAK is sent back so you'll see this received 10 times very quickly
```