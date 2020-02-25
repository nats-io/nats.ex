defmodule Gnat.Async do
  def start_link(settings) do
    children = [
      {Gnat.Async.Queue, settings},
      terminator_child_spec(settings)
    ]
    options = [strategy: :rest_for_one]
    Supervisor.start_link(children, options)
  end

  @type pub_error :: :shutting_down | :queue_full | :memory_full
  @spec pub(atom(), String.t(), binary(), keyword()) :: :ok | {:error, pub_error()}
  def pub(name, topic, message, opts \\ []) do
    Gnat.Async.Queue.pub(name, topic, message, opts)
  end

  defp terminator_child_spec(settings) do
    import Supervisor.Spec
    settings = terminator_settings(settings)
    worker(Gnat.Async.Terminator, [settings], shutdown: settings.graceful_shutdown_timeout + 500)
  end

  defp terminator_settings(%{name: name}=settings) do
    %{
      name: name,
      graceful_shutdown_timeout: Map.get(settings, :graceful_shutdown_timeout, 30_000)
    }
  end
end
