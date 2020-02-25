defmodule Gnat.Async.Queue do
  @moduledoc false

  use GenServer
  require Logger

  @default_queue_settings %{max_messages: 10_000, max_memory: 100 * 1024 * 1024}
  @max_in_flight 10
  @pause 25
  @shutting_down_status :shutting_down

  def start_link(%{name: name}=settings) do
    GenServer.start_link(__MODULE__, settings, [name: name])
  end

  def pub(pid, topic, message, opts \\ []) do
    GenServer.call(pid, {:pub, topic, message, opts})
  end

  @impl GenServer
  def init(%{connection_name: name}=queue_settings) when is_atom(name) do
    queue_settings =
      @default_queue_settings
      |> Map.merge(queue_settings)
      |> Map.put(:queue, :queue.new())
      |> Map.put(:in_flight, %{})
      |> Map.put(:shutdown_pid, nil)
      |> Map.put(:status, :running)
    {:ok, queue_settings, 0}
  end

  @impl GenServer
  def handle_call({:pub, _top, _msg, _opts}, _from, %{status: @shutting_down_status}=state) do
    {:reply, {:error, :shutting_down}, state, 0}
  end
  def handle_call({:pub, _topic, _message, _opts}=todo, _from, state) do
    state = update_in(state, [:queue], fn(q) -> :queue.in(todo, q) end)
    {:reply, :ok, state, 0}
  end

  @impl GenServer
  def handle_info(:timeout, state) do
    if :queue.is_empty(state.queue) do
      if state.status == @shutting_down_status do
        Logger.info("#{__MODULE__} Completed Graceful Shutdown of #{state.name}")
        Process.send(state.shutdown_pid, :finished_shutdown, [])
        {:stop, :normal, state}
      else
        {:noreply, state, @pause}
      end
    else
      {state, timeout} = try_to_send_message(state)
      {:noreply, state, timeout}
    end
  end
  def handle_info({ref, :ok}, state) do
    in_flight = Map.delete(state.in_flight, ref)
    {:noreply, %{state | in_flight: in_flight}, 0}
  end
  def handle_info({:shutdown, pid}, state) do
    Logger.info("#{__MODULE__} starting graceful shutdown")
    {:noreply, %{state | status: @shutting_down_status, shutdown_pid: pid}, 0}
  end
  def handle_info(other, state) do
    Logger.error("#{__MODULE__} Received Unexpected Message: #{inspect(other)}")
    {:noreply, state, 0}
  end

  defp try_to_send_message(%{queue: queue, connection_name: name, in_flight: in_flight}=state) do
    # :queue.is_empty is checked above
    {{:value, pub}, new_queue} = :queue.out(queue)
    case Enum.count(in_flight) do
      n when n < @max_in_flight ->
        case attempt_pub(name, pub) do
          {:ok, ref} ->
            in_flight = Map.put(in_flight, ref, {:erlang.monotonic_time(), pub})
            {%{state | in_flight: in_flight, queue: new_queue}, 0}
          :connection_down ->
            {state, @pause}
        end
      _ -> {state, @pause}
    end
  end

  defp attempt_pub(connection_name, pub) do
    ref = :erlang.make_ref()
    process_message = {:"$gen_call", {self(), ref}, pub}
    try do
      :ok = Process.send(connection_name, process_message, [])
      {:ok, ref}
    rescue
      ArgumentError -> :connection_down
    end
  end
end
