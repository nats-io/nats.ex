# bench/kv_consume.exs
#
# Compares three approaches for consuming all messages from a KV bucket into ETS:
#
#   1. Pager (batch 500, ack_policy: :all) — fetch a page, process, ack last, repeat
#   2. Pull + ack_next pipeline (initial batch 500, ack_policy: :explicit) — prime the
#      pipeline with a batch request, then ack_next each message to keep flow continuous
#   3. PullConsumer with batch_size (ack_policy: :all) — the new batch mode using the
#      actual PullConsumer behaviour, batches messages and acks only the last per batch
#
# Prerequisites:
#   - NATS server with JetStream enabled: nats-server -js
#   - Run with: mix run bench/kv_consume.exs
#
# Optional env vars:
#   - BENCH_COUNT: number of messages (default 100000)
#   - BENCH_BATCH: batch size (default 500)
#   - BENCH_TIME: seconds per scenario (default 60)

Logger.configure(level: :warning)

require Logger
Logger.configure(level: :warning)

alias Gnat.Jetstream.API.{Consumer, KV}
alias Gnat.Jetstream.API.Util

defmodule BenchBatchPullConsumer do
  use Gnat.Jetstream.PullConsumer

  def start(args) do
    Gnat.Jetstream.PullConsumer.start(__MODULE__, args)
  end

  @impl true
  def init(%{tab: tab, notify: pid, expected: expected, batch_size: batch_size}) do
    consumer = %Consumer{
      stream_name: "KV_BENCH_KV",
      ack_policy: :all,
      ack_wait: 30_000_000_000,
      deliver_policy: :all,
      replay_policy: :instant
    }

    {:ok, %{tab: tab, notify: pid, expected: expected, received: 0},
     connection_name: :gnat_bench, consumer: consumer, batch_size: batch_size}
  end

  @impl true
  def handle_message(message, state) do
    :ets.insert(state.tab, {message.topic, message.body})
    received = state.received + 1

    if received >= state.expected do
      send(state.notify, {:done, received})
    end

    {:ack, %{state | received: received}}
  end
end

defmodule KVConsumeBench do
  @bucket "BENCH_KV"
  @stream "KV_BENCH_KV"
  @value_size 64

  def setup(conn, count) do
    # Clean up previous state
    KV.delete_bucket(conn, @bucket)
    :timer.sleep(500)

    {:ok, _} = KV.create_bucket(conn, @bucket, history: 1)

    IO.puts("Populating #{count} messages (#{@value_size} byte values)...")
    start = System.monotonic_time(:millisecond)

    Enum.each(1..count, fn i ->
      key = "key.#{String.pad_leading(Integer.to_string(i), 7, "0")}"
      value = :crypto.strong_rand_bytes(@value_size) |> Base.encode64()
      :ok = KV.put_value(conn, @bucket, key, value)

      if rem(i, 10_000) == 0 do
        elapsed = System.monotonic_time(:millisecond) - start
        rate = round(i / elapsed * 1000)
        IO.puts("  #{i}/#{count} (#{rate} msg/s)")
      end
    end)

    elapsed = System.monotonic_time(:millisecond) - start
    IO.puts("Setup complete: #{count} messages in #{div(elapsed, 1000)}s\n")
  end

  # ---------------------------------------------------------------------------
  # Strategy 1: Pager (ack_policy: :all, batch fetch, ack last per page)
  # ---------------------------------------------------------------------------
  def pager_consume(conn, batch_size) do
    tab = :ets.new(:pager_cache, [:set])

    {:ok, _} =
      Gnat.Jetstream.Pager.reduce(conn, @stream, [batch: batch_size], nil, fn msg, acc ->
        :ets.insert(tab, {msg.topic, msg.body})
        acc
      end)

    count = :ets.info(tab, :size)
    :ets.delete(tab)
    count
  end

  # ---------------------------------------------------------------------------
  # Strategy 2: Pull + ack_next pipeline (ack_policy: :explicit, continuous)
  # ---------------------------------------------------------------------------
  def pull_ack_next_consume(conn, batch_size) do
    {:ok, consumer_info} =
      Consumer.create(conn, %Consumer{
        stream_name: @stream,
        ack_policy: :explicit,
        deliver_policy: :all,
        replay_policy: :instant,
        inactive_threshold: 30_000_000_000
      })

    total = consumer_info.num_pending
    inbox = Util.reply_inbox()
    {:ok, sub} = Gnat.sub(conn, self(), inbox)

    :ok =
      Consumer.request_next_message(
        conn,
        @stream,
        consumer_info.name,
        inbox,
        nil,
        batch: batch_size,
        no_wait: true
      )

    tab = :ets.new(:pull_cache, [:set])
    receive_with_ack_next(sub, inbox, tab, 0, total)

    count = :ets.info(tab, :size)
    :ets.delete(tab)

    Gnat.unsub(conn, sub)
    Consumer.delete(conn, @stream, consumer_info.name)

    count
  end

  @terminals ["404", "408"]

  defp receive_with_ack_next(_sub, _inbox, _tab, total, total), do: :ok

  defp receive_with_ack_next(sub, inbox, tab, count, total) do
    receive do
      {:msg, %{sid: ^sub, status: status}} when status in @terminals ->
        receive_with_ack_next(sub, inbox, tab, count, total)

      {:msg, %{sid: ^sub, reply_to: nil}} ->
        receive_with_ack_next(sub, inbox, tab, count, total)

      {:msg, %{sid: ^sub} = message} ->
        :ets.insert(tab, {message.topic, message.body})

        if count + 1 < total do
          Gnat.Jetstream.ack_next(message, inbox)
        else
          Gnat.Jetstream.ack(message)
        end

        receive_with_ack_next(sub, inbox, tab, count + 1, total)
    after
      30_000 ->
        IO.puts("WARNING: timeout after receiving #{count}/#{total} messages")
        :timeout
    end
  end

  # ---------------------------------------------------------------------------
  # Strategy 3: PullConsumer with batch_size (ack_policy: :all, batch mode)
  #
  # Uses the actual PullConsumer behaviour with the new batch_size option.
  # This is the real-world usage pattern we want to validate.
  # ---------------------------------------------------------------------------
  def batch_pull_consumer_consume(expected, batch_size) do
    tab = :ets.new(:batch_pc_cache, [:set, :public])

    {:ok, pid} =
      BenchBatchPullConsumer.start(%{
        tab: tab,
        notify: self(),
        expected: expected,
        batch_size: batch_size
      })

    receive do
      {:done, _received} -> :ok
    after
      60_000 ->
        IO.puts("WARNING: PullConsumer timeout")
    end

    count = :ets.info(tab, :size)
    Gnat.Jetstream.PullConsumer.close(pid)
    :ets.delete(tab)
    count
  end
end

# -- Configuration -----------------------------------------------------------

count = String.to_integer(System.get_env("BENCH_COUNT", "100000"))
batch = String.to_integer(System.get_env("BENCH_BATCH", "500"))
time = String.to_integer(System.get_env("BENCH_TIME", "60"))

IO.puts("""
KV Consume Benchmark
====================
Messages:   #{count}
Batch size: #{batch}
Time/scenario: #{time}s
""")

# -- Setup --------------------------------------------------------------------

# Named connection for the PullConsumer
conn_settings = %{
  name: :gnat_bench,
  backoff_period: 1_000,
  connection_settings: [%{host: '127.0.0.1', port: 4222}]
}

{:ok, _} = Gnat.ConnectionSupervisor.start_link(conn_settings)
:timer.sleep(500)

# Direct connection for Pager and manual pull
{:ok, conn} = Gnat.start_link(%{host: '127.0.0.1', port: 4222})

KVConsumeBench.setup(conn, count)

# -- Verify all approaches produce correct results ----------------------------

IO.puts("Verifying approaches...")

count1 = KVConsumeBench.pager_consume(conn, batch)
IO.puts("  Pager:              #{count1} entries")

count2 = KVConsumeBench.pull_ack_next_consume(conn, batch)
IO.puts("  Pull+ack_next:      #{count2} entries")

count3 = KVConsumeBench.batch_pull_consumer_consume(count, batch)
IO.puts("  Batch PullConsumer: #{count3} entries")

expected_counts = [count1, count2, count3]

if Enum.any?(expected_counts, &(&1 != count)) do
  IO.puts("\nERROR: expected #{count} entries from each approach")
  Gnat.stop(conn)
  System.halt(1)
end

IO.puts("\nAll approaches verified. Starting benchmark...\n")

# -- Benchmark ---------------------------------------------------------------

Benchee.run(
  %{
    "pager (batch #{batch})" => fn ->
      KVConsumeBench.pager_consume(conn, batch)
    end,
    "pull+ack_next (initial batch #{batch})" => fn ->
      KVConsumeBench.pull_ack_next_consume(conn, batch)
    end,
    "batch_pull_consumer (batch #{batch})" => fn ->
      KVConsumeBench.batch_pull_consumer_consume(count, batch)
    end
  },
  time: time,
  warmup: 0,
  memory_time: 0,
  formatters: [{Benchee.Formatters.Console, comparisons: true}]
)

Gnat.stop(conn)
