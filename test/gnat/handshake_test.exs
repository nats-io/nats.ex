defmodule Gnat.HandshakeTest do
  use ExUnit.Case, async: true
  alias Gnat.Handshake

  describe "negotiate_settings/2" do
    test "respects server auth_required setting" do
      server_settings = %{auth_required: true}
      user_settings = %{username: "test", password: "secret"}

      result = Handshake.negotiate_settings(server_settings, user_settings)

      assert result[:user] == "test"
      assert result[:pass] == "secret"
    end

    test "allows client to force auth when server doesn't require it" do
      server_settings = %{}
      user_settings = %{username: "test", password: "secret", auth_required: true}

      result = Handshake.negotiate_settings(server_settings, user_settings)

      assert result[:user] == "test"
      assert result[:pass] == "secret"
    end

    test "allows client to force auth with token when server doesn't require it" do
      server_settings = %{}
      user_settings = %{token: "my-secret-token", auth_required: true}

      result = Handshake.negotiate_settings(server_settings, user_settings)

      assert result[:auth_token] == "my-secret-token"
    end

    test "doesn't send auth when neither server nor client requires it" do
      server_settings = %{}
      user_settings = %{username: "test", password: "secret"}

      result = Handshake.negotiate_settings(server_settings, user_settings)

      refute Map.has_key?(result, :user)
      refute Map.has_key?(result, :pass)
      refute Map.has_key?(result, :auth_token)
    end

    test "client auth_required setting takes precedence over server setting being false" do
      server_settings = %{auth_required: false}
      user_settings = %{username: "test", password: "secret", auth_required: true}

      result = Handshake.negotiate_settings(server_settings, user_settings)

      assert result[:user] == "test"
      assert result[:pass] == "secret"
    end

    test "works with nkey authentication when client forces auth" do
      nonce = "test-nonce-value"
      server_settings = %{nonce: nonce}

      user_settings = %{
        nkey_seed: "SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTC4",
        auth_required: true
      }

      result = Handshake.negotiate_settings(server_settings, user_settings)

      assert Map.has_key?(result, :sig)
      assert Map.has_key?(result, :nkey)
      assert result[:protocol] == 1
    end

    test "works with JWT+nkey authentication when client forces auth" do
      nonce = "test-nonce-value"
      server_settings = %{nonce: nonce}

      user_settings = %{
        nkey_seed: "SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTC4",
        jwt:
          "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJPUkhQUERHQ1FHRVdPSkZOUVIzM0tFSzVYT0lHSElNNlFOTVFOUUVIVlJLWVpGUkQ3NFNBIiwiaWF0IjoxNjM4MzMzMjI4LCJpc3MiOiJBQlpMM1pSRkdYNTQzRkU1SkdDMkVFQkJRVVhSREQ1TFdWN1dYSEdCSEdOUko2Nks0VUNJUEFHMyIsIm5hbWUiOiJ0ZXN0LXVzZXIiLCJzdWIiOiJVQzJGRllPUTVQWUEyQU5aREFCV1daSEhNRE5JVVdLQ0VITldNSUNCNlo2U1hLNEdOVUFZUUdCUCIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fSwic3VicyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjotMSwidHlwZSI6InVzZXIiLCJ2ZXJzaW9uIjoyfX0.test-signature",
        auth_required: true
      }

      result = Handshake.negotiate_settings(server_settings, user_settings)

      assert Map.has_key?(result, :sig)
      assert Map.has_key?(result, :jwt)
      assert result[:protocol] == 1
    end
  end
end
