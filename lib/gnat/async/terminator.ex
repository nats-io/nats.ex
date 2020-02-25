defmodule Gnat.Async.Terminator do
  @moduledoc false

  use GenServer

  def start_link(settings) do
    GenServer.start_link(__MODULE__, settings)
  end

  def init(%{name: _name}=settings) do
    Process.flag(:trap_exit, true)
    {:ok, settings}
  end

  def terminate(:shutdown, state) do
    Process.send(state.name, :shutdown, [])
    :timer.sleep(5_000)
  end
end
