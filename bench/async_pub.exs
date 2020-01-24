defmodule BenchPublisher do
  def async_publish(num_senders, messages_per_sender) do
    {:ok, _pid} = Gnat.start_link(%{}, name: :gnat)
    {:ok, _pid} = Gnat.AsyncPub.start_link(%{connection_name: :gnat}, name: :async_pub)

    (1..num_senders)
    |> Enum.map(fn(_i) -> kick_off_async_publish(messages_per_sender) end)
    |> Enum.map(fn(task) -> Task.await(task) end)
  end

  def sync_publish(num_senders, messages_per_sender) do
    {:ok, pid} = Gnat.start_link()

    (1..num_senders)
    |> Enum.map(fn(_i) -> kick_off_sync_publish(pid, messages_per_sender) end)
    |> Enum.map(fn(task) -> Task.await(task) end)

    Gnat.stop(pid)
  end

  def kick_off_async_publish(num_messages) do
    Task.async(fn ->
      (1..num_messages)
      |> Enum.each(fn(_i) ->
        Gnat.AsyncPub.pub(:async_pub, "bench", :erlang.monotonic_time() |> Integer.to_string())
      end)
    end)
  end

  def kick_off_sync_publish(pid, num_messages) do
    Task.async(fn ->
      (1..num_messages)
      |> Enum.each(fn(_i) ->
        Gnat.pub(pid, "bench", :erlang.monotonic_time() |> Integer.to_string())
      end)
    end)
  end

  def cleanup_async_pubs do
    Gnat.stop(:gnat)
    Gnat.AsyncPub.stop(:async_pub)
  end
end

defmodule BenchSubscriber do
  def listen_for(how_many_messages, started_at) do
    listen_for(how_many_messages, [], started_at)
  end

  def listen_for(0, samples, started_at) do
    duration = :erlang.monotonic_time() - started_at
    scenario = %Benchee.Scenario{
      job_name: "",
      run_time_data: %Benchee.CollectionData{
        samples: samples,
      },
      memory_usage_data: %Benchee.CollectionData{
        samples: []
      },
      input_name: "Input",
      input: "Input"
    }
    suite = %Benchee.Suite{scenarios: [scenario]}
    stats = Benchee.Statistics.statistics(suite).scenarios |> List.first() |> Map.get(:run_time_data) |> Map.get(:statistics)
    IO.puts "\t\tCompleted #{stats.sample_size} messages in #{duration / 1_000_000}ms"
    %{50 => median, 99 => ninety_ninth} = stats.percentiles
    messages_per_second = (stats.sample_size / duration) * :erlang.convert_time_unit(1, :second, :native)

    IO.puts "\t\t#{:erlang.round(messages_per_second)} msg/sec, median: #{:erlang.round(median / 1_000_000)}ms, 99%: #{:erlang.round(ninety_ninth / 1_000_000)}ms"
  end
  def listen_for(how_many_messages, list, started_at) do
    receive do
      {:msg, %{body: timestamp_str}} ->
        timestamp = String.to_integer(timestamp_str)
        latency = :erlang.monotonic_time() - timestamp
        listen_for(how_many_messages - 1, [latency | list], started_at)
    after
      1_000 -> raise "Timed out waiting for messages"
    end
  end
end

{:ok, sub_conn} = Gnat.start_link()
{:ok, _ref} = Gnat.sub(sub_conn, self(), "bench")

scenarios = [
  {10, 10_000},
  {100, 1_000},
  {1_000, 100},
]

Enum.each(scenarios, fn({num_senders, messages_per_sender}) ->
  total_messages = num_senders * messages_per_sender
  IO.puts "## #{num_senders} publishers"
  IO.puts "\tSync"
  started_at = :erlang.monotonic_time()
  BenchPublisher.sync_publish(num_senders, messages_per_sender)
  BenchSubscriber.listen_for(total_messages, started_at)

  IO.puts "\tAsync"
  started_at = :erlang.monotonic_time()
  BenchPublisher.async_publish(num_senders, messages_per_sender)
  BenchSubscriber.listen_for(total_messages, started_at)
  BenchPublisher.cleanup_async_pubs()
end)

