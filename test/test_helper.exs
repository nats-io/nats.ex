ExUnit.configure(exclude: [:pending, :multi_server])

ExUnit.start()

case :gen_tcp.connect('localhost', 4222, [:binary]) do
  {:ok, socket} ->
    :gen_tcp.close(socket)
  {:error, reason} ->
    Mix.raise "Cannot connect to gnatsd" <>
              " (http://localhost:4222):" <>
              " #{:inet.format_error(reason)}\n" <>
              "You probably need to start gnatsd."
end

defmodule CheckForExpectedNatsServers do
  def check(tags) do
    check_for_default()
    Enum.each(tags, &check_for_tag/1)
  end

  def check_for_default do
    case :gen_tcp.connect('localhost', 4222, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to gnatsd" <>
                  " (tcp://localhost:4222):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start gnatsd."
    end
  end

  def check_for_tag(:multi_server) do
    case :gen_tcp.connect('localhost', 4223, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to gnatsd" <>
                  " (tcp://localhost:4223):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a gnatsd " <>
                  "server that requires authentication with " <>
                  "the following command `gnatsd -p 4223 " <>
                  "--user bob --pass alice`."
    end

    case :gen_tcp.connect('localhost', 4224, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to gnatsd" <>
                  " (tcp://localhost:4224):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a gnatsd " <>
                  "server that requires tls with " <>
                  "a command like `gnatsd -p 4224 " <>
                  "--tls --tlscert test/fixtures/server.pem " <>
                  "--tlskey test/fixtures/key.pem`."
    end
  end
  def check_for_tag(_), do: :ok
end
