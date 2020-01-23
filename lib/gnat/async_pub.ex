defmodule Gnat.AsyncPub do
  use GenServer
  require Logger

  @default_queue_settings %{max_messages: 10_000, max_memory: 100 * 1024 * 1024}
  @pause 25

  def start_link(queue_settings \\ %{}, opts \\ []) do
    GenServer.start_link(__MODULE__, queue_settings, opts)
  end

  @spec pub(GenServer.server, String.t, binary(), keyword()) :: :ok
  def pub(pid, topic, message, opts \\ []) do
    GenServer.call(pid, {:pub, topic, message, opts})
  end

  @impl GenServer
  def init(%{connection_name: name}=queue_settings) when is_atom(name) do
    queue_settings =
      @default_queue_settings
      |> Map.merge(queue_settings)
      |> Map.put(:queue, :queue.new())
    {:ok, queue_settings, 0}
  end

  @impl GenServer
  def handle_call({:pub, _topic, _message, _opts}=todo, _from, state) do
    state = update_in(state, [:queue], fn(q) -> :queue.in(todo, q) end)
    {:reply, :ok, state, 0}
  end

  @impl GenServer
  def handle_info(:timeout, state) do
    if :queue.is_empty(state.queue) do
      {:noreply, state, @pause}
    else
      new_queue = try_to_send_message(state)
      state = Map.put(state, :queue, new_queue)
      {:noreply, state, 0}
    end
  end

  defp try_to_send_message(%{queue: queue, connection_name: name}) do
    # :queue.is_empty is checked above
    {{:value, {:pub, topic, message, opts} = todo}, queue} = :queue.out(queue)
    case attempt_pub(name, topic, message, opts) do
      :ok -> queue
      :error -> :queue.in(todo, queue)
    end
  end

  defp attempt_pub(connection_name, topic, message, opts) do
    try do
      Gnat.pub(connection_name, topic, message, opts)
    catch
      :exit, _msg -> :error
    end
  end
end
