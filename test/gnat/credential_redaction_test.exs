defmodule Gnat.CredentialRedactionTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  @moduletag capture_log: true

  setup do
    id = System.unique_integer([:positive])

    credentials = %{
      username: "redaction-user-#{id}",
      password: "redaction-password-#{id}",
      token: "redaction-token-#{id}",
      jwt: "redaction-jwt-#{id}",
      nkey_seed: "redaction-seed-#{id}",
      ssl_opts: [
        password: ~c"redaction-tls-password-#{id}",
        key: {:PrivateKeyInfo, "redaction-private-key-#{id}"},
        user_lookup_fun: {fn _, _, state -> {:ok, state} end, "redaction-callback-#{id}"}
      ]
    }

    secrets =
      Enum.map([:password, :token, :jwt, :nkey_seed], &credentials[&1]) ++
        [
          "redaction-tls-password-#{id}",
          "redaction-private-key-#{id}",
          "redaction-callback-#{id}"
        ]

    %{
      settings: Map.merge(credentials, %{name: "redaction-test", host: "127.0.0.1", tls: false}),
      secrets: secrets
    }
  end

  test "supervisor connection logs redact credentials", %{settings: settings, secrets: secrets} do
    log =
      capture_log(fn ->
        supervisor = start_connection_supervisor(settings)
        assert %{gnat: gnat} = :sys.get_state(supervisor)
        assert is_pid(gnat)
        assert_settings(gnat, settings)
        stop_supervised!(Gnat.ConnectionSupervisor)
      end)

    assert log =~ "connecting to"
    assert log =~ "redaction-test"
    assert log =~ settings.username
    assert_redacted(log, secrets)
  end

  test "failed connection attempts redact credentials", %{settings: settings, secrets: secrets} do
    log =
      capture_log(fn ->
        supervisor = start_connection_supervisor(Map.put(settings, :port, 0))
        assert %{gnat: nil} = :sys.get_state(supervisor)
        stop_supervised!(Gnat.ConnectionSupervisor)
      end)

    assert log =~ "connecting to"
    assert log =~ "failed to connect"
    assert log =~ ~r/failed to connect :(econnrefused|eaddrnotavail)/
    assert_redacted(log, secrets)
  end

  test "supervisor logs redact credentials in the first element of exception tuples", %{
    settings: settings,
    secrets: secrets
  } do
    supervisor = start_connection_supervisor(settings)
    :sys.get_state(supervisor)

    log =
      capture_log(fn ->
        send(supervisor, {{:connection_error, settings}, []})
        :sys.get_state(supervisor)
      end)

    assert log =~ "connection_error"
    assert log =~ "127.0.0.1"
    assert_redacted(log, secrets)
  end

  test "supervisor initialization errors redact connection settings", %{
    settings: settings,
    secrets: secrets
  } do
    Process.flag(:trap_exit, true)

    {result, log} =
      with_log(fn ->
        Gnat.ConnectionSupervisor.start_link(%{connection_settings: [settings]})
      end)

    assert_redacted(inspect(result, limit: :infinity) <> log, secrets)
    assert {:error, {%RuntimeError{message: message}, stacktrace}} = result
    assert message =~ "KeyError"

    assert Enum.all?(stacktrace, fn {_module, _function, arity, _location} ->
             is_integer(arity)
           end)
  end

  for mode <- [:direct, :supervised],
      scenario <- [
        :tls_key,
        :tls_password,
        :tls_callback,
        :nkey_charlist,
        :nkey_nonce,
        :nkey_nonce_callback
      ] do
    test "#{mode} startup errors redact #{scenario} credentials" do
      Process.flag(:trap_exit, true)
      secret = "startup-secret-#{System.unique_integer([:positive])}"

      {credentials, nonce, secret} =
        case unquote(scenario) do
          :tls_key ->
            {%{tls: true, ssl_opts: [verify: :verify_none, key: secret]}, "nonce", secret}

          :tls_password ->
            {%{tls: true, ssl_opts: [verify: :verify_none, password: {:invalid, secret}]},
             "nonce", secret}

          :tls_callback ->
            {%{
               tls: true,
               ssl_opts: [verify: :verify_none, user_lookup_fun: {fn _ -> :unused end, secret}]
             }, "nonce", secret}

          :nkey_charlist ->
            seed = nkey_seed()
            {%{nkey_seed: String.to_charlist(seed)}, "nonce", seed}

          :nkey_nonce_callback ->
            seed = nkey_seed()
            {%{nkey_seed: fn -> seed end}, 123, seed}

          :nkey_nonce ->
            seed = nkey_seed()
            {%{nkey_seed: seed}, 123, seed}
        end

      {server, port} = start_wire_server(nonce, false)
      settings = Map.merge(credentials, %{host: "127.0.0.1", port: port})

      {result, log} =
        with_log(fn ->
          case unquote(mode) do
            :direct ->
              Gnat.start_link(settings)

            :supervised ->
              supervisor = start_connection_supervisor(settings)
              assert %{gnat: nil} = :sys.get_state(supervisor)
              stop_supervised!(Gnat.ConnectionSupervisor)
          end
        end)

      assert_redacted(inspect(result, limit: :infinity) <> log, [secret])

      if unquote(mode) == :direct do
        assert {:error, {%RuntimeError{message: message}, stacktrace}} = result
        assert message =~ "connection initialization raised"
        assert stacktrace != []

        assert Enum.all?(stacktrace, fn {_module, _function, arity, _location} ->
                 is_integer(arity)
               end)
      else
        assert log =~ "failed to connect"
        assert log =~ "connection initialization raised"
      end

      send(server, :stop)
    end
  end

  for mode <- [:direct, :supervised], key <- [:password, :token, :nkey_seed] do
    test "#{mode} startup errors redact exceptions from #{key} callbacks" do
      Process.flag(:trap_exit, true)
      secret = "callback-secret-#{System.unique_integer([:positive])}"
      {server, port} = start_wire_server("nonce", false)

      settings = %{
        unquote(key) => fn -> raise secret end,
        host: "127.0.0.1",
        port: port
      }

      settings =
        if unquote(key) == :password,
          do: Map.put(settings, :username, "callback-user"),
          else: settings

      {result, log} =
        with_log(fn ->
          case unquote(mode) do
            :direct ->
              Gnat.start_link(settings)

            :supervised ->
              supervisor = start_connection_supervisor(settings)
              assert %{gnat: nil} = :sys.get_state(supervisor)
              stop_supervised!(Gnat.ConnectionSupervisor)
          end
        end)

      assert_redacted(inspect(result, limit: :infinity) <> log, [secret])

      if unquote(mode) == :direct do
        assert {:error, {%RuntimeError{message: message}, _stacktrace}} = result
        assert message =~ "connection initialization raised RuntimeError"
      else
        assert log =~ "failed to connect"
      end

      send(server, :stop)
    end
  end

  for module <- [Gnat, Gnat.ConnectionSupervisor] do
    test "#{inspect(module)} status redacts credentials without changing live settings", %{
      settings: settings,
      secrets: secrets
    } do
      pid = start_process(unquote(module), settings)
      status = pid |> :sys.get_status() |> inspect(limit: :infinity, printable_limit: :infinity)

      assert status =~ "redaction-test"
      assert status =~ "connection_settings"
      assert status =~ "127.0.0.1"
      assert status =~ "tls: false"
      assert_redacted(status, secrets)
      assert_settings(pid, settings)
    end

    test "#{inspect(module)} formatted event history redacts credentials", %{
      settings: settings,
      secrets: secrets
    } do
      pid = start_process(unquote(module), settings)
      log_state(unquote(module), pid)

      {:status, ^pid, _, [_dictionary, _system_state, _parent, _raw_debug, formatted]} =
        :sys.get_status(pid)

      status = inspect(formatted, limit: :infinity, printable_limit: :infinity)
      assert status =~ "Logged events"
      assert status =~ "redaction-test"
      assert_redacted(status, secrets)
      assert_settings(pid, settings)
    end

    test "#{inspect(module)} crash reports redact credentials", %{
      settings: settings,
      secrets: secrets
    } do
      pid = start_process(unquote(module), settings)
      log_state(unquote(module), pid)

      log =
        capture_log(fn ->
          monitor = Process.monitor(pid)
          :sys.terminate(pid, :redaction_test_failure)
          assert_receive {:DOWN, ^monitor, :process, ^pid, :redaction_test_failure}
        end)

      assert log =~ "redaction_test_failure"
      assert log =~ "redaction-test"
      assert_redacted(log, secrets)
    end
  end

  test "redaction preserves each authentication method on the wire", %{settings: settings} do
    seed = nkey_seed()
    {:ok, keypair} = NKEYS.from_seed(seed)
    nonce = "redaction-auth-nonce"
    signature = keypair |> NKEYS.sign(nonce) |> Base.url_encode64(padding: false)

    for callback? <- [false, true],
        {credentials, expected} <- [
          {Map.take(settings, [:username, :password]),
           %{"user" => settings.username, "pass" => settings.password}},
          {%{token: settings.token}, %{"auth_token" => settings.token}},
          {%{nkey_seed: seed}, %{"nkey" => NKEYS.public_nkey(keypair), "sig" => signature}},
          {%{nkey_seed: seed, jwt: settings.jwt}, %{"jwt" => settings.jwt, "sig" => signature}}
        ] do
      credentials =
        if callback? do
          Map.new(credentials, fn
            {key, value} when key in [:password, :token, :nkey_seed] -> {key, fn -> value end}
            pair -> pair
          end)
        else
          credentials
        end

      {server, port} = start_wire_server(nonce)
      settings = Map.merge(credentials, %{host: "127.0.0.1", port: port})
      supervisor = start_connection_supervisor(settings)
      assert %{gnat: gnat} = :sys.get_state(supervisor)
      assert is_pid(gnat)
      assert_receive {:connect, ^server, connect}
      assert Map.take(connect, Map.keys(expected)) == expected
      assert_settings(gnat, settings)
      stop_supervised!(Gnat.ConnectionSupervisor)
      send(server, :stop)
    end
  end

  defp log_state(module, pid) do
    :ok = :sys.log(pid, true)

    case module do
      Gnat -> Gnat.server_info(pid)
      Gnat.ConnectionSupervisor -> send(pid, :redaction_test_event)
    end

    :sys.get_state(pid)
  end

  defp start_process(Gnat, settings) do
    start_supervised!(Supervisor.child_spec({Gnat, settings}, restart: :temporary))
  end

  defp start_process(Gnat.ConnectionSupervisor, settings) do
    start_connection_supervisor(settings)
  end

  defp start_connection_supervisor(settings) do
    start_supervised!(
      Supervisor.child_spec(
        {Gnat.ConnectionSupervisor,
         %{
           name: :credential_redaction_connection,
           backoff_period: 60_000,
           connection_settings: [settings]
         }},
        restart: :temporary
      )
    )
  end

  defp assert_settings(pid, settings) do
    case :sys.get_state(pid).connection_settings do
      [actual] -> assert actual == settings
      actual -> assert Map.take(actual, Map.keys(settings)) == settings
    end
  end

  defp assert_redacted(output, secrets) do
    for secret <- secrets, do: refute(output =~ secret)
    assert output =~ ~r/\[REDACTED\]|:redacted/
  end

  defp nkey_seed do
    bytes = <<18::5, 20::5, 0::6, :crypto.strong_rand_bytes(32)::binary>>
    Base.encode32(<<bytes::binary, NKEYS.CRC.compute(bytes)::little-16>>)
  end

  defp start_wire_server(nonce, expect_connect \\ true) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :line])
    {:ok, {_, port}} = :inet.sockname(listener)
    owner = self()

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)

        :ok =
          :gen_tcp.send(
            socket,
            "INFO " <> Jason.encode!(%{auth_required: true, nonce: nonce}) <> "\r\n"
          )

        if expect_connect do
          {:ok, "CONNECT " <> connect} = :gen_tcp.recv(socket, 0, 1_000)
          send(owner, {:connect, self(), Jason.decode!(connect)})
        end

        receive do
          :stop -> :gen_tcp.close(socket)
        end
      end)

    on_exit(fn ->
      :gen_tcp.close(listener)
      Process.exit(server, :kill)
    end)

    {server, port}
  end
end
