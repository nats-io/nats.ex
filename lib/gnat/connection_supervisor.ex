defmodule Gnat.ConnectionSupervisor do
  use GenServer
  require Logger

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

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

  def handle_info(:attempt_connection, state) do
    connection_config = random_connection_config(state)
    Logger.info "connecting to #{inspect connection_config}"
    case Gnat.start_link(connection_config, name: state.name) do
      {:ok, gnat} -> {:noreply, %{state | gnat: gnat}}
      {:error, err} ->{:noreply, %{state | gnat: nil}} # we will get an :EXIT message and handle it there
    end
  end
  def handle_info({:EXIT, _pid, reason}, %{gnat: nil}=state) do
    Logger.error "failed to connect #{inspect reason}"
    Process.send_after(self(), :attempt_connection, state.backoff_period)
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

