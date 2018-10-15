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
      {:msg, %{topic: "echo", reply_to: reply_to, body: "ping"}} ->
        Gnat.pub(gnat, reply_to, "pong")

      other ->
        IO.puts("server received: #{inspect(other)}")
    end

    loop(gnat)
  end
end

{:ok, pid} = Gnat.start_link(%{host: '127.0.0.1', port: 4222})
EchoServer.run(pid)

msg128 = :crypto.strong_rand_bytes(128)

msg16 = "74c93e71c5aa03ad"

tcp_packet = "MSG topic 1 128\r\n#{msg128}\r\n"

parser = Gnat.Parser.new()

Benchee.run(
  %{
    "parse-128" => fn ->
      {_parser, [_msg]} = Gnat.Parser.parse(parser, tcp_packet)
    end,
    "pub - 128" => fn -> :ok = Gnat.pub(pid, "pub128", msg128) end,
    "sub-unsub-pub-16" => fn ->
      rand = :crypto.strong_rand_bytes(8) |> Base.encode64()
      {:ok, subscription} = Gnat.sub(pid, self(), rand)
      :ok = Gnat.unsub(pid, subscription, max_messages: 1)
      :ok = Gnat.pub(pid, rand, msg16)

      receive do
        {:msg, %{topic: ^rand, body: ^msg16}} -> :ok
      after
        100 -> raise "timed out on sub"
      end
    end,
    "req-reply-4" => fn ->
      {:ok, %{body: "pong"}} = Gnat.request(pid, "echo", "ping")
    end
  },
  time: 10,
  parallel: 1,
  console: [comparison: false]
)
