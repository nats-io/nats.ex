defmodule Gnat.Handshake do
  alias Gnat.Parser

  @moduledoc """
  This module provides a single function which handles all of the variations of establishing a connection to a gnatsd server and just returns {:ok, socket} or {:error, reason}
  """
  def connect(settings) do
    case result = :gen_tcp.connect(settings.host, settings.port, settings.tcp_opts, settings.timeout) do
      {:ok, tcp} -> perform_handshake(tcp, settings)
      _ -> result
    end
  end

  defp perform_handshake(tcp, connection_settings) do
    receive do
      {:tcp, ^tcp, operation} ->
        {_, [{:info, options}]} = Parser.parse(Parser.new, operation)
        {:ok, socket} = upgrade_connection(tcp, options, connection_settings)
        send_connect_message(socket, options, connection_settings)
        {:ok, socket}
      after 1000 ->
        {:error, "timed out waiting for info"}
    end
  end

  defp socket_write(%{tls: true}, socket, iodata), do: :ssl.send(socket, iodata)
  defp socket_write(_, socket, iodata), do: :gen_tcp.send(socket, iodata)

  defp send_connect_message(socket, %{auth_required: true}=_options, %{username: username, password: password}=connection_settings) do
    opts = Poison.Encoder.encode(%{user: username, pass: password, verbose: false}, strict_keys: true)
    socket_write(connection_settings, socket, "CONNECT #{opts}\r\n")
  end
  defp send_connect_message(socket, _options, connection_settings) do
    socket_write(connection_settings, socket, "CONNECT {\"verbose\": false}\r\n")
  end

  defp upgrade_connection(tcp, %{tls_required: true}, %{tls: true, ssl_opts: opts}) do
    :ok = :inet.setopts(tcp, [active: true])
    :ssl.connect(tcp, opts, 1_000)
  end
  defp upgrade_connection(tcp, _server_settings, _connection_settions), do: {:ok, tcp}
end
