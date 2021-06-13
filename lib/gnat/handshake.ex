defmodule Gnat.Handshake do
  alias Gnat.Parsec

  @moduledoc """
  This module provides a single function which handles all of the
  variations of establishing a connection to a nats server and just
  returns {:ok, socket} or {:error, reason}
  """
  def connect(settings) do
    host = settings.host |> to_charlist
    case :gen_tcp.connect(host, settings.port, settings.tcp_opts, settings.connection_timeout) do
      {:ok, tcp} -> perform_handshake(tcp, settings)
      result -> result
    end
  end

  @doc false
  def negotiate_settings(server_settings, user_settings) do
    %{verbose: false}
    |> negotiate_auth(server_settings, user_settings)
  end

  defp perform_handshake(tcp, user_settings) do
    receive do
      {:tcp, ^tcp, operation} ->
        {_, [{:info, server_settings}]} = Parsec.parse(Parsec.new(), operation)
        {:ok, socket} = upgrade_connection(tcp, user_settings)
        settings = negotiate_settings(server_settings, user_settings)
        :ok = send_connect(user_settings, settings, socket)
        {:ok, socket}
      after 1000 ->
        {:error, "timed out waiting for info"}
    end
  end

  defp send_connect(%{tls: true}, settings, socket) do
    :ssl.send(socket, "CONNECT " <> Jason.encode!(settings, maps: :strict) <> "\r\n")
  end
  defp send_connect(_, settings, socket) do
    :gen_tcp.send(socket, "CONNECT " <> Jason.encode!(settings, maps: :strict) <> "\r\n")
  end

  defp negotiate_auth(settings, %{auth_required: true}=_server, %{username: username, password: password}=_user) do
    Map.merge(settings, %{user: username, pass: password})
  end
  defp negotiate_auth(settings, %{auth_required: true}=_server, %{token: token}=_user) do
    Map.merge(settings, %{auth_token: token})
  end
  defp negotiate_auth(settings, %{auth_required: true, nonce: nonce}=_server, %{nkey_seed: seed, jwt: jwt}=_user) do
    {:ok, nkey} = NKEYS.from_seed(seed)
    signature = NKEYS.sign(nkey, nonce) |> Base.url_encode64() |> String.replace("=", "")

    Map.merge(settings, %{sig: signature, protocol: 1, jwt: jwt})
  end
  defp negotiate_auth(settings, %{auth_required: true, nonce: nonce}=_server, %{nkey_seed: seed}=_user) do
    {:ok, nkey} = NKEYS.from_seed(seed)
    signature = NKEYS.sign(nkey, nonce) |> Base.url_encode64() |> String.replace("=", "")
    public = NKEYS.public_nkey(nkey)

    Map.merge(settings, %{sig: signature, protocol: 1, nkey: public})
  end
  defp negotiate_auth(settings, _server, _user) do
    settings
  end

  defp upgrade_connection(tcp, %{tls: true, ssl_opts: opts}) do
    :ok = :inet.setopts(tcp, [active: true])
    :ssl.connect(tcp, opts, 1_000)
  end
  defp upgrade_connection(tcp, _settings), do: {:ok, tcp}
end
