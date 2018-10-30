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
        IO.puts "server received: #{inspect other}"
    end

    loop(gnat)
  end
end

{:ok, pid} = Gnat.start_link(%{host: '127.0.0.1', port: 4222})
EchoServer.run(pid)

msg128="74c93e71c5aa03ad4f0881caa374ba1af08f3e4a04ce5f8bd0b2d82d6d72de6eef3e46ed8d8c3dbe24d0f6109115dcdf13280d1c13c2f6d22d14336b29df8e65"
msg16="74c93e71c5aa03ad"

tcp_packet = "MSG topic 1 128\r\n#{msg128}\r\n"
Benchee.run(%{
 "parse-128" => fn -> {_parser, [_msg]} = Gnat.Parser.new() |> Gnat.Parser.parse(tcp_packet) end,
  "pub - 128" => fn -> :ok = Gnat.pub(pid, "pub128", msg128) end,
 "sub-unsub-pub-16" => fn ->
   rand = :crypto.strong_rand_bytes(8) |> Base.encode64
   {:ok, subscription} = Gnat.sub(pid, self(), rand)
   :ok = Gnat.unsub(pid, subscription, max_messages: 1)
   :ok = Gnat.pub(pid, rand, msg16)
   receive do
     {:msg, %{topic: ^rand, body: ^msg16}} -> :ok
     after 100 -> raise "timed out on sub"
   end
 end,
 "req-reply-4" => fn ->
   {:ok, %{body: "pong"}} = Gnat.request(pid, "echo", "ping")
 end,
}, time: 10, parallel: 1, console: [comparison: false])
