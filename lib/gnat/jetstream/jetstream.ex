defmodule Gnat.Jetstream do
  @moduledoc """
  Provides functions for interacting with a [NATS Jetstream](https://github.com/nats-io/jetstream)
  server.
  """

@doc """
Sends `AckAck` acknowledgement to the server.

Acknowledges a message was completely handled.
"""
@spec ack(message :: Gnat.message()) :: :ok
def ack(message)

def ack(%{gnat: gnat, reply_to: reply_to}) do
  Gnat.pub(gnat, reply_to, "")
end

@doc """
Sends `AckNext` acknowledgement to the server.

Acknowledges the message was handled and requests delivery of the next message to the reply
subject. Only applies to Pull-mode.
"""
@spec ack_next(message :: Gnat.message(), consumer_subject :: binary()) :: :ok
def ack_next(message, consumer_subject)

def ack_next(%{gnat: gnat, reply_to: reply_to}, consumer_subject) do
  Gnat.pub(gnat, reply_to, "+NXT", reply_to: consumer_subject)
end

@doc """
Sends `AckNak` acknowledgement to the server.

Signals that the message will not be processed now and processing can move onto the next message.
NAK'd message will be retried.
"""
@spec nack(message :: Gnat.message()) :: :ok
def nack(message)

def nack(%{gnat: gnat, reply_to: reply_to}) do
  Gnat.pub(gnat, reply_to, "-NAK")
end

@doc """
Sends `AckProgress` acknowledgement to the server.

When sent before the `AckWait` period indicates that work is ongoing and the period should be
extended by another equal to `AckWait`.
"""
@spec ack_progress(message :: Gnat.message()) :: :ok
def ack_progress(message)

def ack_progress(%{gnat: gnat, reply_to: reply_to}) do
  Gnat.pub(gnat, reply_to, "+WPI")
end

@doc """
Sends `AckTerm` acknowledgement to the server.

Instructs the server to stop redelivery of a message without acknowledging it as successfully
processed.
"""
@spec ack_term(message :: Gnat.message()) :: :ok
def ack_term(message)

def ack_term(%{gnat: gnat, reply_to: reply_to}) do
  Gnat.pub(gnat, reply_to, "+TERM")
end
end
