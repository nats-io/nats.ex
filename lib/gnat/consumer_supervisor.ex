defmodule Gnat.ConsumerSupervisor do
  use GenServer
  require Logger

  @moduledoc """
  A process that can supervise consumers for you

  If you want to subscribe to a few topics and have that subscription last across restarts for you, then this worker can be of help. It also spawns a supervised `Task` for each message it receives. This way errors in message processing don't crash the consumers, but you will still get SASL reports that you can send to services like honeybadger.

  To use this just add an entry to your supervision tree like this:

  ```
  consumer_supervisor_settings = %{
    connection_name: :name_of_supervised_connection,
    consuming_function: {MyApp.RpcServer, :handle_request},
    subscription_topics: [
      %{topic: "rpc.MyApp.search", queue_group: "rpc.MyApp.search"},
      %{topic: "rpc.MyApp.create", queue_group: "rpc.MyApp.create"},
    ],
  }
  worker(Gnat.ConsumerSupervisor, [consumer_supervisor_settings, [name: :rpc_consumer]], shutdown: 30_000)
  ```

  The second argument is a keyword list that gets used as the GenServer options so you can pass a name that you want to register for the consumer process if you like. The `consuming_function` specific which module and function to call when messages arrive. The function will be called with a single argument which is a gnat message just like you get when you call `Gnat.sub` directly.

  You can have a single consumer that subscribes to multiple topics or multiple consumers that subscribe to different topics and call different consuming functions. It is recommended that your `ConsumerSupervisor`s are present later in your supervision tree than your `ConnectionSupervisor`. That way during a shutdown the `ConsumerSupervisor` can attempt a graceful shutdown of the consumer before shutting down the connection.
  """

  def start_link(settings, options \\ []) do
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
  # Ignore DOWN and task result messages from the spawned tasks
  def handle_info({:DOWN, _ref, :process, _task_pid, _reason}, state), do: {:noreply, state}
  def handle_info({ref, _result}, state) when is_reference(ref), do: {:noreply, state}

  def handle_info({:msg, gnat_message}, %{consuming_function: {mod, fun}}=state) do
    Task.Supervisor.async_nolink(state.task_supervisor_pid, mod, fun, [gnat_message])
    {:noreply, state}
  end

  def handle_info(other, state) do
    Logger.error "#{__MODULE__} received unexpected message #{inspect other}"
    {:noreply, state}
  end

  def terminate(_reason, _state) do
    # TODO unsub, then wait for the task supervisor to be empty
  end
end
