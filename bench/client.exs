Application.put_env(:client, :num_connections, 4)
num_requesters = 16
requests_per_requester = 500

defmodule Client do
  require Logger

  def setup(id) do
    num_connections = Application.get_env(:client, :num_connections)
    partition = rem(id, num_connections)
    String.to_atom("gnat#{partition}")
  end

  def send_request(gnat, request) do
    {:ok, _} = Gnat.request(gnat, "echo", request)# |> IO.inspect
  end

  def send_requests(gnat, how_many, request) do
    :lists.seq(1, how_many)
    |> Enum.each(fn(_) ->
      {micro_seconds, _result} = :timer.tc(fn() -> send_request(gnat, request) end)
      Benchmark.record_rpc_time(micro_seconds)
    end)
  end
end

defmodule Benchmark do
  def benchmark(num_actors, requests_per_actor, request) do
    {:ok, _pid} = Agent.start_link(fn -> [] end, name: __MODULE__)
    {total_micros, _result} = time_benchmark(num_actors, requests_per_actor, request)
    total_requests = num_actors * requests_per_actor
    total_bytes = total_requests * byte_size(request) * 2
    print_statistics(total_requests, total_bytes, total_micros)
    Agent.stop(__MODULE__, :normal)
  end

  def record_rpc_time(micro_seconds) do
    Agent.update(__MODULE__, fn(list) -> [micro_seconds | list] end)
  end

  def print_statistics(total_requests, total_bytes, total_micros) do
    total_seconds = total_micros / 1_000_000.0
    req_throughput = total_requests / total_seconds
    kilobyte_throughput = total_bytes / 1024 / total_seconds
    IO.puts "It took #{total_seconds}sec"
    IO.puts "\t#{req_throughput} req/sec"
    IO.puts "\t#{kilobyte_throughput} kb/sec"
    Agent.get(__MODULE__, fn(list_of_rpc_times) ->
      tc_l = list_of_rpc_times
      tc_n = Enum.count(list_of_rpc_times)
      tc_min = :lists.min(tc_l)
      tc_max = :lists.max(tc_l)
      sorted = :lists.sort(tc_l)
      tc_med = :lists.nth(round(tc_n * 0.5), sorted)
      tc_90th = :lists.nth(round(tc_n * 0.9), sorted)
      tc_avg = round(Enum.sum(tc_l) / tc_n)
      IO.puts "\tmin: #{tc_min}µs"
      IO.puts "\tmax: #{tc_max}µs"
      IO.puts "\tmedian: #{tc_med}µs"
      IO.puts "\t90th percentile: #{tc_90th}µs"
      IO.puts "\taverage: #{tc_avg}µs"
      IO.puts "\t#{tc_min},#{tc_max},#{tc_med},#{tc_90th},#{tc_avg},#{req_throughput},#{kilobyte_throughput}"
    end)
  end

  def time_benchmark(num_actors, requests_per_actor, request) do
    :timer.tc(fn() ->
      (1..num_actors) |> Enum.map(fn(i) ->
        parent = self()
        spawn(fn() ->
          gnat = Client.setup(i)
          #IO.puts "starting requests #{i}"
          Client.send_requests(gnat, requests_per_actor, request)
          #IO.puts "done with requests #{i}"
          send parent, :ack
        end)
      end)
      wait_for_times(num_actors)
    end)
  end

  def wait_for_times(0), do: :done
  def wait_for_times(n) do
    receive do
      :ack ->
        wait_for_times(n-1)
    end
  end
end

num_connections = Application.get_env(:client, :num_connections)
Enum.each(0..(num_connections - 1), fn(i) ->
  name = :"gnat#{i}"
  {:ok, _pid} = Gnat.start_link(%{}, name: name)
end)
:timer.sleep(500) # let the connections get started

#request = "ping"
request = :crypto.strong_rand_bytes(16)
Benchmark.benchmark(num_requesters, requests_per_requester, request)
