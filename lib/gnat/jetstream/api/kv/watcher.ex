defmodule Gnat.Jetstream.API.KV.Watcher do
  @moduledoc """
  The watcher server establishes a subscription to the changes that occur to a given key-value bucket. The
  consumer-supplied handler function will be sent an indicator as to whether the change is a delete or an add,
  as well as the key being changed and the value (if it was added).

  Ensure that you call `stop` with a watcher pid when you no longer need to be notified about key changes
  """
  use GenServer

  alias Gnat.Jetstream.API.{Consumer, KV, Util}
  alias Gnat.Jetstream.API.KV.Entry

  @type keywatch_handler ::
          (action :: :key_deleted | :key_added, key :: String.t(), value :: any() -> nil)

  @type watcher_options ::
          {:conn, Gnat.t()}
          | {:bucket_name, String.t()}
          | {:handler, keywatch_handler()}

  @spec start_link(opts :: [watcher_options()]) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def stop(pid) do
    GenServer.stop(pid)
  end

  def init(opts) do
    {:ok, {sub, consumer_name}} = subscribe(opts[:conn], opts[:bucket_name])

    {:ok,
     %{
       handler: opts[:handler],
       conn: opts[:conn],
       bucket_name: opts[:bucket_name],
       sub: sub,
       consumer_name: consumer_name,
       domain: Keyword.get(opts, :domain)
     }}
  end

  def terminate(_reason, state) do
    stream = KV.stream_name(state.bucket_name)
    :ok = Gnat.unsub(state.conn, state.sub)
    :ok = Consumer.delete(state.conn, stream, state.consumer_name, state.domain)
  end

  def handle_info({:msg, message}, state) do
    case Entry.from_message(message, state.bucket_name) do
      {:ok, entry} ->
        state.handler.(action(entry.operation), entry.key, entry.value)

      :ignore ->
        :ok
    end

    {:noreply, state}
  end

  defp action(:put), do: :key_added
  defp action(:delete), do: :key_deleted
  defp action(:purge), do: :key_purged

  defp subscribe(conn, bucket_name) do
    stream = KV.stream_name(bucket_name)
    inbox = Util.reply_inbox()
    consumer_name = "all_key_values_watcher_#{Util.nuid()}"

    with {:ok, sub} <- Gnat.sub(conn, self(), inbox),
         {:ok, _consumer} <-
           Consumer.create(conn, %Consumer{
             durable_name: consumer_name,
             deliver_subject: inbox,
             stream_name: stream,
             ack_policy: :none,
             max_ack_pending: -1,
             max_deliver: 1
           }) do
      {:ok, {sub, consumer_name}}
    end
  end
end
