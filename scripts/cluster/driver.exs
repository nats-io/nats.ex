# Manual failover driver for the 3-node cluster under scripts/cluster/.
#
# Usage (from repo root, after `scripts/cluster/cluster.sh start`):
#
#   mix run --no-halt scripts/cluster/driver.exs
#
# The script:
#   * connects to 2 of the 3 cluster nodes via Gnat.ConnectionSupervisor
#   * creates (if needed) a 3-replica stream "FAILOVER_STREAM" and a durable
#     consumer "FAILOVER_CONSUMER"
#   * starts a PullConsumer that logs every delivery
#   * publishes a numbered message every second
#
# Watch the logs, then in another terminal run:
#   scripts/cluster/cluster.sh kill n1
#   scripts/cluster/cluster.sh start n1
#   scripts/cluster/cluster.sh kill n2
# …and verify the consumer keeps receiving messages.

require Logger
Logger.configure(level: :info)

alias Gnat.Jetstream.API.{Consumer, Stream}

stream_name = "FAILOVER_STREAM"
consumer_name = "FAILOVER_CONSUMER"
subject = "failover.test"

# Deliberately point at only 2 of the 3 nodes so we can prove the connection
# can still reach the cluster even when one of the listed nodes is down.
connection_settings = [
  %{host: ~c"127.0.0.1", port: 4223},
  %{host: ~c"127.0.0.1", port: 4224}
]

{:ok, _sup} =
  Gnat.ConnectionSupervisor.start_link(
    %{
      name: :gnat,
      backoff_period: 1_000,
      connection_settings: connection_settings
    },
    name: :gnat_sup
  )

# Wait until the connection is actually established before issuing JS admin calls.
wait_for_gnat = fn wait_for_gnat ->
  case Process.whereis(:gnat) do
    nil ->
      Process.sleep(100)
      wait_for_gnat.(wait_for_gnat)

    pid when is_pid(pid) ->
      :ok
  end
end

wait_for_gnat.(wait_for_gnat)
Logger.info("connected to cluster")

# Create (or reuse) a 3-replica stream + durable consumer.
stream = %Stream{
  name: stream_name,
  subjects: [subject],
  num_replicas: 3,
  storage: :file
}

case Stream.create(:gnat, stream) do
  {:ok, _} -> Logger.info("created stream #{stream_name}")
  {:error, %{"err_code" => 10058}} -> Logger.info("stream #{stream_name} already exists")
  {:error, other} -> Logger.warning("stream create returned: #{inspect(other)}")
end

consumer = %Consumer{
  stream_name: stream_name,
  durable_name: consumer_name,
  ack_policy: :all
}

case Consumer.create(:gnat, consumer) do
  {:ok, _} -> Logger.info("created consumer #{consumer_name}")
  {:error, %{"err_code" => 10148}} -> Logger.info("consumer #{consumer_name} already exists")
  {:error, other} -> Logger.warning("consumer create returned: #{inspect(other)}")
end

# ---------- The PullConsumer under test ----------
defmodule FailoverConsumer do
  use Gnat.Jetstream.PullConsumer
  require Logger

  def start_link(opts), do: Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    {:ok, %{count: 0, batch_size: 10}, opts}
  end

  @impl true
  def handle_message(%{body: body, reply_to: reply_to}, state) do
    # reply_to carries delivery metadata: $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts>.<pending>
    meta =
      case reply_to do
        "$JS.ACK." <> rest -> rest
        other -> other
      end

    Logger.info("RECV ##{state.count + 1} body=#{inspect(body)} meta=#{meta}")
    {:ack, %{state | count: state.count + 1}}
  end

  @impl true
  def handle_status(%{status: status, description: desc}, state) do
    Logger.warning("STATUS #{status} #{desc}")
    {:ok, state}
  end

  def handle_status(%{status: status}, state) do
    Logger.warning("STATUS #{status}")
    {:ok, state}
  end

  @impl true
  def handle_connected(consumer_info, state) do
    Logger.info("connected, num_pending=#{consumer_info.num_pending}")
    {:ok, state}
  end
end

{:ok, _pc} =
  FailoverConsumer.start_link(
    connection_name: :gnat,
    stream_name: stream_name,
    consumer_name: consumer_name
  )

Logger.info("PullConsumer started; publishing one message per second")

# ---------- Publisher loop ----------
# Uses Gnat.request so we wait for the JetStream publish ack. We only bump
# the accepted counter when the server confirms the write — that way RECV
# numbers track actual stream writes, not best-effort pub attempts.
defmodule Publisher do
  require Logger

  def loop(subject, attempt \\ 1, accepted \\ 0) do
    body = "msg-#{accepted + 1}-#{System.system_time(:millisecond)}"

    result =
      try do
        Gnat.request(:gnat, subject, body, receive_timeout: 2_000)
      catch
        :exit, reason -> {:error, {:exit, reason}}
      end

    case result do
      {:ok, %{body: resp_body}} ->
        case Jason.decode(resp_body) do
          {:ok, %{"stream" => stream, "seq" => seq}} ->
            Logger.info("PUB  ##{accepted + 1} ok stream=#{stream} seq=#{seq} body=#{body}")
            Process.sleep(1_000)
            loop(subject, attempt + 1, accepted + 1)

          {:ok, %{"error" => err}} ->
            Logger.warning(
              "PUB attempt ##{attempt} rejected by server: #{inspect(err)} body=#{body}"
            )

            Process.sleep(1_000)
            loop(subject, attempt + 1, accepted)

          other ->
            Logger.warning("PUB attempt ##{attempt} unparsable ack: #{inspect(other)}")
            Process.sleep(1_000)
            loop(subject, attempt + 1, accepted)
        end

      {:error, reason} ->
        Logger.warning("PUB attempt ##{attempt} failed: #{inspect(reason)} body=#{body}")
        Process.sleep(1_000)
        loop(subject, attempt + 1, accepted)
    end
  end
end

Publisher.loop(subject)
