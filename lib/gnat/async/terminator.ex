defmodule Gnat.Async.Terminator do
  @moduledoc false

  require Logger
  use GenServer

  def start_link(settings) do
    GenServer.start_link(__MODULE__, settings)
  end

  def init(%{name: _name}=settings) do
    Process.flag(:trap_exit, true)
    {:ok, settings}
  end

  def terminate(:shutdown, state) do
    Process.send(state.name, {:shutdown, self()}, [])
    receive do
      :finished_shutdown -> nil
    after
      state.graceful_shutdown_timeout ->
        Logger.error("#{__MODULE__} Timed out waiting for the #{state.name} async queue to drain")
        :error_logger.error_report([
          type: :gnat_async_terminator_timeout,
          message: "Timed out waiting for the #{state.name} async queue to drain"
        ])
    end
  end

  def terminator(reason, _state) do
    Logger.error("#{__MODULE__} shutting down with unexpected reason #{inspect(reason)}")
  end
end
