defmodule Gnat.Jetstream.ConnCase do
  @moduledoc """
  This module defines the test case to be used by tests that require setting up a connection.
  """

  use ExUnit.CaseTemplate

  using(opts) do
    tags = module_tags(opts)

    quote do
      import Gnat.Jetstream.ConnCase

      @moduletag unquote(tags)
    end
  end

  defp module_tags(opts) do
    Enum.reduce(opts, [capture_log: true], fn opt, acc ->
      add_module_tag(acc, opt)
    end)
  end

  defp add_module_tag(tags, {:min_server_version, min_version}) do
    if server_version_incompatible?(min_version) do
      Keyword.put(tags, :incompatible, true)
    else
      tags
    end
  end

  defp add_module_tag(tags, _opt), do: tags

  defp get_server_version(conn) do
    Gnat.server_info(conn).version
  end

  defp server_version_incompatible?(min_version) do
    {:ok, conn} = Gnat.start_link()
    match? = Version.match?(get_server_version(conn), ">= #{min_version}")
    :ok = Gnat.stop(conn)
    !match?
  end

  setup tags do
    if arg = Map.get(tags, :with_gnat) do
      conn = start_gnat!(arg)

      %{conn: conn}
    else
      :ok
    end
  end

  def start_gnat!(name) when is_atom(name) do
    start_gnat!(%{}, name: name)
  end

  def start_gnat!(connection_settings, options)
      when is_map(connection_settings) and is_list(options) do
    {Gnat, %{}}
    |> Supervisor.child_spec(start: {Gnat, :start_link, [connection_settings, options]})
    |> start_supervised!()
  end
end
