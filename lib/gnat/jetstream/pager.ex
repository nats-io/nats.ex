defmodule Gnat.Jetstream.Pager do
  @moduledoc false

  alias Gnat.Jetstream
  alias Gnat.Jetstream.API.{Consumer, Util}

  @opaque pager :: map()
  @type message :: Gnat.message()

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
      inactive_threshold: 30_000_000_000
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
  defp apply_opts_to_consumer(consumer, opts) do
    case Keyword.get(opts, :from_seq) do
      nil ->
        consumer

      seq when is_integer(seq) ->
        %Consumer{consumer | deliver_policy: :by_start_sequence, opt_start_seq: seq}
    end
  end
end
