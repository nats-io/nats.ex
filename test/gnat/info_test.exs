defmodule Gnat.InfoTest do
  use ExUnit.Case, async: true

  alias Gnat.Parsec

  test "INFO keeps unknown and nested keys as strings without creating atoms" do
    {info, keys} = extension_info()
    assert_no_atoms(keys)

    {parser, [{:info, decoded}]} = Parsec.parse(Parsec.new(), info_frame(info))

    assert_no_atoms(keys)
    assert parser.partial == nil
    assert decoded == info
  end

  test "INFO maps supported top-level fields to fixed atoms" do
    info = %{
      acc_is_sys: false,
      api_lvl: 5,
      auth_required: true,
      client_id: 123,
      client_ip: "127.0.0.1",
      cluster: "test",
      cluster_dynamic: false,
      connect_info: true,
      connect_urls: ["127.0.0.1:4222"],
      domain: "test-domain",
      git_commit: "abc123",
      go: "go1.25.0",
      headers: true,
      host: "127.0.0.1",
      ip: "127.0.0.1",
      jetstream: true,
      ldm: false,
      max_payload: 1_048_576,
      nonce: "test-nonce",
      port: 4222,
      proto: 1,
      remote_account: "test-account",
      server_id: "test-server-id",
      server_name: "test-server",
      ssl_required: false,
      tls_available: true,
      tls_required: false,
      tls_verify: false,
      version: "2.15.0",
      ws_connect_urls: ["127.0.0.1:8080"],
      xkey: "server-x25519-public-key"
    }

    assert {_, [{:info, ^info}]} = Parsec.parse(Parsec.new(), info_frame(info))
  end

  test "initial INFO preserves extensions and negotiates authentication and headers" do
    {extensions, keys} = extension_info()
    assert_no_atoms(keys)

    known_info = %{
      auth_required: true,
      headers: true,
      domain: "test-domain",
      xkey: "server-x25519-public-key",
      api_lvl: 5
    }

    info = Map.merge(extensions, known_info)

    {conn, socket, connect} =
      connect_peer(info, %{username: "user", password: "pass", no_responders: true})

    assert_no_atoms(keys)
    assert Gnat.server_info(conn) == Map.merge(extensions, known_info)

    assert %{"user" => "user", "pass" => "pass", "headers" => true, "no_responders" => true} =
             connect

    assert :ok = :gen_tcp.send(socket, "PING\r\n")
    assert {:ok, "PONG\r\n"} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "initial INFO nonce is used for nkey authentication" do
    nonce = "test-nonce"
    seed = "SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTC4"
    info = %{"auth_required" => true, "nonce" => nonce}
    {conn, _socket, connect} = connect_peer(info, %{nkey_seed: seed})
    {:ok, nkey} = NKEYS.from_seed(seed)

    assert Gnat.server_info(conn) == %{auth_required: true, nonce: nonce}
    assert connect["nkey"] == NKEYS.public_nkey(nkey)
    assert connect["sig"] == Base.url_encode64(NKEYS.sign(nkey, nonce), padding: false)
    assert connect["protocol"] == 1
  end

  test "subsequent INFO preserves extensions and updates known fields" do
    {conn, socket, _connect} = connect_peer(%{"version" => "initial"}, %{})
    assert Gnat.server_info(conn) == %{version: "initial"}
    {extensions, keys} = extension_info()
    assert_no_atoms(keys)

    known_info = %{
      version: "updated",
      max_payload: 2048,
      connect_info: true,
      remote_account: "test-account",
      acc_is_sys: true
    }

    info = Map.merge(extensions, known_info)

    assert :ok = :gen_tcp.send(socket, [info_frame(info), "PING\r\n"])
    assert {:ok, "PONG\r\n"} = :gen_tcp.recv(socket, 0, 1_000)

    assert_no_atoms(keys)

    assert Gnat.server_info(conn) ==
             Map.merge(extensions, known_info)
  end

  defp extension_info do
    suffix = Base.encode16(:crypto.strong_rand_bytes(16))
    keys = Enum.map(["top", "nested", "array"], &"info_#{&1}_#{suffix}")
    [top, nested, array] = keys

    {%{
       top => %{nested => [%{array => true, "version" => "nested"}]},
       "error" => [%{"headers" => false, "domain" => "nested", "connect_info" => true}]
     }, keys}
  end

  defp assert_no_atoms(keys) do
    for key <- keys do
      assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
    end
  end

  defp info_frame(info), do: "INFO " <> Jason.encode!(info) <> "\r\n"

  defp connect_peer(info, settings) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, packet: :line, ip: {127, 0, 0, 1}])

    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    owner = self()

    peer =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 1_000)
        :ok = :gen_tcp.send(socket, info_frame(info))
        {:ok, "CONNECT " <> connect} = :gen_tcp.recv(socket, 0, 1_000)
        {:ok, "SUB _INBOX." <> _} = :gen_tcp.recv(socket, 0, 1_000)
        :ok = :gen_tcp.controlling_process(socket, owner)
        {socket, Jason.decode!(connect)}
      end)

    conn = start_supervised!({Gnat, Map.merge(settings, %{host: "127.0.0.1", port: port})})
    {socket, connect} = Task.await(peer)
    {conn, socket, connect}
  end
end
