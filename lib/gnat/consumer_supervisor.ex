defmodule Gnat.ConsumerSupervisor do
  use GenServer
  require Logger

  def start_link(settings, options) do
    GenServer.start_link(__MODULE__, settings, options)
  end

  def init(settings) do
    Process.flag(:trap_exit, true)
    {:ok, task_supervisor_pid} = Task.Supervisor.start_link()
    connection_name = Map.get(settings, :connection_name)
    subscription_topics = Map.get(settings, :subscription_topics)
    consuming_function = Map.get(settings, :consuming_function)
    send self(), :connect
    state = %{
      connection_name: connection_name,
      connection_pid: nil,
      consuming_function: consuming_function,
      status: :disconnected,
      subscription_topics: subscription_topics,
      subscriptions: [],
      task_supervisor_pid: task_supervisor_pid,
    }
    {:ok, state}
  end

  def handle_info(:connect, %{connection_name: name}=state) do
    case Process.whereis(name) do
      nil ->
        Process.send_after(self(), :connect, 2_000)
        {:noreply, state}
      connection_pid ->
        _ref = Process.monitor(connection_pid)
        subscriptions = Enum.map(state.subscription_topics, fn(topic_and_queue_group) ->
          topic = Map.fetch!(topic_and_queue_group, :topic)
          queue_group = Map.get(topic_and_queue_group, :queue_group)
          {:ok, subscription} = Gnat.sub(connection_pid, self(), topic, queue_group: queue_group)
          subscription
        end)
        {:noreply, %{state | status: :connected, connection_pid: connection_pid, subscriptions: subscriptions}}
    end
  end

  def handle_info({:DOWN, _ref, :process, connection_pid, _reason}, %{connnection_pid: connection_pid}=state) do
    Process.send_after(self(), :connect, 2_000)
    {:noreply, %{state | status: :disconnected, connection_pid: nil, subscriptions: []}}
  end

  def handle_info({:msg, gnat_message}, %{consuming_function: {mod, fun}}=state) do
    Task.Supervisor.async_nolink(state.task_supervisor_pid, mod, fun, [gnat_message])
    {:noreply, state}
  end

  def terminate(_reason, _state) do
    # TODO unsub, then wait for the task supervisor to be empty
  end
end
