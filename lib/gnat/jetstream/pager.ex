defmodule Gnat.Jetstream.Pager do
  @moduledoc """
  Page through all the messages in a stream

  This module provides a synchronous API to inspect the messages in a stream.
  You can use the reduce module to write a simple function that works like `Enum.reduce` across each message individually.
  If you want to handle messages in batches, you can use the `init` + `page` functions to accomplish that.
  """

  alias Gnat.Jetstream
  alias Gnat.Jetstream.API.{Consumer, Util}

  @opaque pager :: map()
  @type message :: Gnat.message()
  @type opts :: list(opt())

  @typedoc """
  Options you can pass to the pager

  * `batch` controls the maximum number of messages we'll pull in each page/batch (default 10)
  * `domain` You can specify a jetstream domain if needed
  * `from_datetime` Only page through messages recorded on or after this datetime
  * `from_seq` Only page through messages with a sequence number equal or above this option
  * `headers_only` You can pass `true` to this if you only want to see the headers from each message. Can be useful to get metadata without having to receieve large body payloads.

  """
  @type opt ::
          {:batch, non_neg_integer()}
          | {:domain, String.t()}
          | {:from_datetime, DateTime.t()}
          | {:from_seq, non_neg_integer}
          | {:headers_only, boolean()}

  @spec init(Gnat.t(), String.t(), opts()) :: {:ok, pager()} | {:error, term()}
  def init(conn, stream_name, opts) do
    domain = Keyword.get(opts, :domainl)

    consumer = %Consumer{
      stream_name: stream_name,
      domain: domain,
      ack_policy: :all,
      ack_wait: 30_000_000_000,
      deliver_policy: :all,
      description: "ephemeral consumer",
      replay_policy: :instant,
      inactive_threshold: 30_000_000_000,
      headers_only: Keyword.get(opts, :headers_only)
    }

    consumer = apply_opts_to_consumer(consumer, opts)

    inbox = Util.reply_inbox()

    with {:ok, consumer_info} <- Consumer.create(conn, consumer),
         {:ok, sub} <- Gnat.sub(conn, self(), inbox) do
      state = %{
        conn: conn,
        stream_name: stream_name,
        consumer_name: consumer_info.name,
        domain: domain,
        inbox: inbox,
        batch: Keyword.get(opts, :batch, 10),
        sub: sub
      }

      {:ok, state}
    end
  end

  @spec page(pager()) :: {:page, list(message())} | {:done, list(message())} | {:error, term()}
  def page(state) do
    with :ok <- request_next_message(state) do
      receive_messages(state, [])
    end
  end

  def cleanup(%{conn: conn} = state) do
    with :ok <- Gnat.unsub(conn, state.sub) do
      :ok = Consumer.delete(conn, state.stream_name, state.consumer_name, state.domain)
    end
  end

  @doc """
  Similar to Enum.reduce but you can iterate through all messages in a stream

  ```
  # Assume we have a stream with messages like "1", "2", ... "10"
  Gnat.Jetstream.Pager.reduce(:gnat, "NUMBERS_STREAM", [batch_size: 5], 0, fn(message, total) ->
    num = String.to_integer(message.body)
    total + num
  end)

  # => {:ok, 55}
  ```
  """
  @spec reduce(
          Gnat.t(),
          String.t(),
          opts(),
          Enum.acc(),
          (Gnat.message(), Enum.acc() -> Enum.acc())
        ) :: {:ok, Enum.acc()} | {:error, term()}
  def reduce(conn, stream_name, opts, initial_state, fun) do
    with {:ok, pager} <- init(conn, stream_name, opts) do
      page_through(pager, initial_state, fun)
    end
  end

  defp page_through(pager, state, fun) do
    case page(pager) do
      {:page, messages} ->
        new_state = Enum.reduce(messages, state, fun)
        page_through(pager, new_state, fun)

      {:done, messages} ->
        new_state = Enum.reduce(messages, state, fun)
        :ok = cleanup(pager)
        {:ok, new_state}
    end
  end

  defp request_next_message(state) do
    opts = [batch: state.batch, no_wait: true]

    Consumer.request_next_message(
      state.conn,
      state.stream_name,
      state.consumer_name,
      state.inbox,
      state.domain,
      opts
    )
  end

  defp receive_messages(%{batch: batch}, messages) when length(messages) == batch do
    last = hd(messages)
    :ok = Jetstream.ack(last)
    {:page, Enum.reverse(messages)}
  end

  @terminals ["404", "408"]
  defp receive_messages(%{sub: sid} = state, messages) do
    receive do
      {:msg, %{sid: ^sid, status: status}} when status in @terminals ->
        {:done, Enum.reverse(messages)}

      {:msg, %{sid: ^sid, reply_to: nil}} ->
        {:done, Enum.reverse(messages)}

      {:msg, %{sid: ^sid} = message} ->
        receive_messages(state, [message | messages])
    end
  end

  ## Helpers for accepting user options
  defp apply_opts_to_consumer(consumer = %Consumer{}, opts) do
    from = {Keyword.get(opts, :from_seq), Keyword.get(opts, :from_datetime)}

    case from do
      {nil, nil} ->
        consumer

      {seq, _} when is_integer(seq) ->
        %Consumer{consumer | deliver_policy: :by_start_sequence, opt_start_seq: seq}

      {_, %DateTime{} = dt} ->
        %Consumer{consumer | deliver_policy: :by_start_time, opt_start_time: dt}
    end
  end
end
