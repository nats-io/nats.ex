defmodule Gnat.Jetstream.Pager do
  @moduledoc false

  alias Gnat.Jetstream
  alias Gnat.Jetstream.API.{Consumer, Util}

  @opaque pager :: map()
  @type message :: Gnat.message()

  def init(conn, stream_name, opts) do
    name = "gnat_stream_pager_#{Util.nuid()}"

    first_seq = Keyword.fetch!(opts, :from_seq)

    consumer = %Consumer{
      stream_name: stream_name,
      durable_name: name,
      ack_policy: :explicit,
      ack_wait: 30_000_000_000,
      deliver_policy: :by_start_sequence,
      description: "Gnat Stream Pager",
      opt_start_seq: first_seq,
      replay_policy: :instant,
      inactive_threshold: 30_000_000_000
    }
    inbox = Util.reply_inbox()

    with {:ok, _config} <- Consumer.create(conn, consumer),
         {:ok, sub} <- Gnat.sub(conn, self(), inbox) do
      state =
        %{
          conn: conn,
          stream_name: stream_name,
          consumer_name: name,
          domain: nil,
          inbox: inbox,
          batch: 10,
          sub: sub
        }

      {:ok, state}
    end
  end

  @spec page(pager()) :: {:page, list(message())} | {:done, list(message())} | {:error, term()}
  def page(%{conn: conn, batch: batch} = state) do
    opts = [batch: batch, no_wait: true]
    with :ok <- Consumer.request_next_message(conn, state.stream_name, state.consumer_name, state.inbox, state.domain, opts) do
      receive_messages(state, [])
    end
  end

  def cleanup(%{conn: conn} = state) do
    with :ok <- Gnat.unsub(conn, state.sub),
         :ok <- Consumer.delete(conn, state.stream_name, state.consumer_name, state.domain) do
      :ok
    end
  end

  defp receive_messages(%{batch: batch}, messages) when length(messages) == batch do
    {:page, Enum.reverse(messages)}
  end

  defp receive_messages(%{sub: sid} = state, messages) do
    receive do
      {:msg, %{sid: ^sid, status: "408"}} ->
        {:done, Enum.reverse(messages)}

      {:msg, %{sid: ^sid} = message} ->
        with :ok <- Jetstream.ack(message) do
          receive_messages(state, [message | messages ])
        end
    end
  end
end
