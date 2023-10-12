defmodule Gnat.ConnectionSupervisor do
  use GenServer
  require Logger

  @moduledoc """
  A process that can supervise a named connection for you

  If you would like to supervise a Gnat connection and have it automatically re-connect in case of failure you can use this module in your supervision tree.
  It takes a map with the following data:

  ```
  gnat_supervisor_settings = %{
    name: :gnat, # (required) the registered named you want to give the Gnat connection
    backoff_period: 4_000, # number of milliseconds to wait between consecutive reconnect attempts (default: 2_000)
    connection_settings: [
      %{host: '10.0.0.100', port: 4222},
      %{host: '10.0.0.101', port: 4222},
    ]
  }
  ```

  The connection settings can specify all of the same values that you pass to `Gnat.start_link/1`. Each time a connection is attempted we will use one of the provided connection settings to open the connection. This is a simplistic way of load balancing your connections across a cluster of nats nodes and allowing failover to other nodes in the cluster if one goes down.

  To use this in your supervision tree add an entry like this:

  ```
  import Supervisor.Spec
  worker(Gnat.ConnectionSupervisor, [gnat_supervisor_settings, [name: :my_connection_supervisor]])
  ```

  The second argument is used as GenServer options so you can give the supervisor a registered name as well if you like. Now in the rest of your code you can call things like:

  ```
  :ok = Gnat.pub(:gnat, "subject", "message")
  ```

  And it will use your supervised connection. If the connection is down when you call that function (or dies during that function) it will raise an error.
  """
  @spec start_link(map(), keyword()) :: GenServer.on_start
  def start_link(settings, options \\ []) do
    GenServer.start_link(__MODULE__, settings, options)
  end

  @impl GenServer
  def init(options) do
    state = %{
      backoff_period: Map.get(options, :backoff_period, 2000),
      connection_settings: Map.fetch!(options, :connection_settings),
      name: Map.fetch!(options, :name),
      gnat: nil,
    }
    Process.flag(:trap_exit, true)
    send self(), :attempt_connection
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:attempt_connection, state) do
    connection_config = random_connection_config(state)
    Logger.debug "connecting to #{inspect connection_config}"
    case Gnat.start_link(connection_config, name: state.name) do
      {:ok, gnat} -> {:noreply, %{state | gnat: gnat}}
      {:error, err} ->
        Logger.error "failed to connect #{inspect err}"
        Process.send_after(self(), :attempt_connection, state.backoff_period)
        {:noreply, %{state | gnat: nil}}
    end
  end

  # in OTP 25 and below, we will get back an EXIT message in addition to receiving the {:error, reason}
  # tuple on from the start_link call above. So if we get an exit message when there is no connection tracked
  # it means will have already scheduled a new attempt_connection
  def handle_info({:EXIT, _pid, _reason}, %{gnat: nil}=state) do
    {:noreply, state}
  end
  def handle_info({:EXIT, _pid, reason}, state) do
    Logger.error "connection failed #{inspect reason}"
    send self(), :attempt_connection
    {:noreply, state}
  end
  def handle_info(msg, state) do
    Logger.error "#{__MODULE__} received unexpected message #{inspect msg}"
    {:noreply, state}
  end

  defp random_connection_config(%{connection_settings: connection_settings}) do
    connection_settings |> Enum.random()
  end
end
