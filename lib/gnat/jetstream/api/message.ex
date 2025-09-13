defmodule Gnat.Jetstream.API.Message do
  @moduledoc """
  This module provides a way to parse the `reply_to` received by a PullConsumer
  and get some useful information about the state of the consumer.
  """

  # Based on:
  # https://github.com/nats-io/nats.py/blob/d9f24b4beae541b7723873ba0a786ea7c0ecb3d5/nats/aio/msg.py#L182

  defmodule Metadata do
    @type t :: %__MODULE__{
            :stream_seq => integer(),
            :consumer_seq => integer(),
            :num_pending => integer(),
            :num_delivered => integer(),
            :timestamp => DateTime.t(),
            :stream => String.t(),
            :consumer => String.t(),
            :domain => String.t() | nil
          }

    defstruct [
      :stream_seq,
      :consumer_seq,
      :num_pending,
      :num_delivered,
      :timestamp,
      :stream,
      :consumer,
      :domain
    ]
  end

  @spec metadata(message :: Gnat.message()) :: {:ok, Metadata.t()} | {:error, term()}
  def metadata(%{reply_to: "$JS.ACK." <> ack_topic}),
    do: decode_reply_to(String.split(ack_topic, "."))

  def metadata(_), do: {:error, :no_jetstream_message}

  # # Subject without domain:
  # $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
  defp decode_reply_to([
         stream,
         consumer,
         num_delivered,
         stream_seq,
         consumer_seq,
         ts,
         num_pending
       ]) do
    with {ts, ""} <- Integer.parse(ts),
         {:ok, ts} <- DateTime.from_unix(ts, :nanosecond),
         {stream_seq, ""} <- Integer.parse(stream_seq),
         {consumer_seq, ""} <- Integer.parse(consumer_seq),
         {num_delivered, ""} <- Integer.parse(num_delivered),
         {num_pending, ""} <- Integer.parse(num_pending) do
      {:ok,
       %Metadata{
         stream_seq: stream_seq,
         consumer_seq: consumer_seq,
         num_delivered: num_delivered,
         num_pending: num_pending,
         timestamp: ts,
         stream: stream,
         consumer: consumer
       }}
    else
      _ ->
        {:error, :invalid_ack_reply_to}
    end
  end

  # Subject with domain:
  # $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.
  #   <cseq>.<tm>.<pending>.<a token with a random value>
  defp decode_reply_to([
         domain,
         _account_hash,
         stream,
         consumer,
         num_delivered,
         stream_seq,
         consumer_seq,
         ts,
         num_pending,
         _random_value
       ]) do
    case decode_reply_to([
           stream,
           consumer,
           num_delivered,
           stream_seq,
           consumer_seq,
           ts,
           num_pending
         ]) do
      {:ok, metadata} ->
        domain = if domain == "_", do: nil, else: domain
        {:ok, %{metadata | domain: domain}}

      err = {:error, _} ->
        err
    end
  end

  defp decode_reply_to(_), do: {:error, :invalid_ack_reply_to}
end
