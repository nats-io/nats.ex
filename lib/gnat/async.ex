defmodule Gnat.Async do
  def start_link(%{name: name}=settings) do
    children = [
      {Gnat.Async.Queue, settings},
      {Gnat.Async.Terminator, %{name: name}}
    ]
    options = [strategy: :rest_for_one]
    Supervisor.start_link(children, options)
  end

  @type pub_error :: :shutting_down | :queue_full | :memory_full
  @spec pub(atom(), String.t(), binary(), keyword()) :: :ok | {:error, pub_error()}
  def pub(name, topic, message, opts \\ []) do
    Gnat.Async.Queue.pub(name, topic, message, opts)
  end
end
