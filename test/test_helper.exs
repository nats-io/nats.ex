ExUnit.configure(exclude: [pending: true])

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

case :gen_tcp.connect('localhost', 4223, [:binary]) do
  {:ok, socket} ->
    :gen_tcp.close(socket)
  {:error, reason} ->
    Mix.raise "Cannot connect to gnatsd" <>
              " (http://localhost:4223):" <>
              " #{:inet.format_error(reason)}\n" <>
              "You probably need to start a gnatsd " <>
              "server that requires authentication with " <>
              "the following command `gnatsd -p 4223 " <>
              "--user bob --pass alice`."
end