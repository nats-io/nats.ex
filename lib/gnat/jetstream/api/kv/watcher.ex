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

  # Matches the ordered-consumer defaults used by the nats.go KV watcher:
  # a 5s idle heartbeat and server-driven flow control. The flow-control
  # messages arrive as 100-status messages with a reply subject — the client
  # is expected to publish an empty reply so the server releases backpressure.
  @flow_control_heartbeat_ns 5_000_000_000

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

  # Flow-control request: the server is asking us to acknowledge that we're
  # keeping up. Responding releases backpressure so the server continues
  # delivering messages to slow handlers rather than dropping us as a slow
  # consumer.
  def handle_info({:msg, %{status: "100", reply_to: reply_to}}, state)
      when is_binary(reply_to) and reply_to != "" do
    :ok = Gnat.pub(state.conn, reply_to, "")
    {:noreply, state}
  end

  # Idle heartbeat (status 100 with no reply_to) and any other informational
  # status message (404, 408, 409, etc.) — not a stream record, drop it.
  def handle_info({:msg, %{status: status}}, state)
      when is_binary(status) and status != "" do
    {:noreply, state}
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
             max_deliver: 1,
             flow_control: true,
             idle_heartbeat: @flow_control_heartbeat_ns
           }) do
      {:ok, {sub, consumer_name}}
    end
  end
end
