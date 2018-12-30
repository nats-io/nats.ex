defmodule EchoServer do
  def run(gnat) do
    spawn(fn -> init(gnat) end)
  end

  def init(gnat) do
    Gnat.sub(gnat, self(), "echo")
    loop(gnat)
  end

  def loop(gnat) do
    receive do
      {:msg, %{topic: "echo", reply_to: reply_to, body: msg}} ->
        Gnat.pub(gnat, reply_to, msg)
      other ->
        IO.puts "server received: #{inspect other}"
    end

    loop(gnat)
  end
end

{:ok, server_pid} = Gnat.start_link(%{host: '127.0.0.1', port: 4222})
EchoServer.run(server_pid)

inputs = %{
  "16 byte" => :crypto.strong_rand_bytes(16),
  "128 byte" => :crypto.strong_rand_bytes(128),
  "1024 byte" => :crypto.strong_rand_bytes(1024),
}

{:ok, client_pid} = Gnat.start_link(%{host: '127.0.0.1', port: 4222})

Benchee.run(%{
  "request" => fn(msg) -> {:ok, %{body: _}} = Gnat.request(client_pid, "echo", msg) end,
}, time: 10, parallel: 1, console: [comparison: false], inputs: inputs)
