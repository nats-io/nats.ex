defmodule Gnat.ConnectionSupervisorTest do
  use ExUnit.Case, async: true
  @moduletag :capture_log

  @nkey_seed "test/fixtures/nkey_seed" |> File.read!() |> String.trim()

  test "connection secrets are redacted from the process status" do
    secrets = %{password: "supervisor-password", token: "supervisor-token", nkey_seed: @nkey_seed}

    settings = %{
      name: :connection_supervisor_redaction_test,
      backoff_period: 60_000,
      connection_settings: [Map.merge(%{host: "127.0.0.1", port: 1}, secrets)]
    }

    {:ok, supervisor} = Gnat.ConnectionSupervisor.start_link(settings)
    status = inspect(:sys.get_status(supervisor), limit: :infinity, printable_limit: :infinity)

    assert status =~ "connection_settings"
    Enum.each(Map.values(secrets), fn secret -> refute status =~ secret end)
    GenServer.stop(supervisor)
  end

  test "connection settings are logged with their secrets redacted when connecting" do
    import ExUnit.CaptureLog
    secrets = %{password: "logged-password", token: "logged-token", nkey_seed: @nkey_seed}

    settings = %{
      name: :connection_supervisor_logging_test,
      backoff_period: 60_000,
      connection_settings: [
        Map.merge(%{host: "127.0.0.1", port: 1, username: "logged-user"}, secrets)
      ]
    }

    log =
      capture_log([level: :debug], fn ->
        {:ok, supervisor} = Gnat.ConnectionSupervisor.start_link(settings)
        # the first connection attempt has been handled once the supervisor answers
        :sys.get_state(supervisor)
        GenServer.stop(supervisor)
      end)

    assert log =~ "connecting to"
    assert log =~ ~s(username: "logged-user")
    assert log =~ ~s(host: "127.0.0.1")
    assert log =~ "password: :redacted"
    assert log =~ "token: :redacted"
    assert log =~ "nkey_seed: :redacted"
    Enum.each(Map.values(secrets), fn secret -> refute log =~ secret end)
  end
end
