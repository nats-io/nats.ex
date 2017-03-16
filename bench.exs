{:ok, pid} = Neato.start_link(%{host: '127.0.0.1', port: 4222})

msg128="74c93e71c5aa03ad4f0881caa374ba1af08f3e4a04ce5f8bd0b2d82d6d72de6eef3e46ed8d8c3dbe24d0f6109115dcdf13280d1c13c2f6d22d14336b29df8e65"
msg16="74c93e71c5aa03ad"

IO.puts "== Parsing tcp packets"
tcp_packet = "MSG topic 1 128\r\n#{msg128}\r\n"
Benchee.run(%{
  "parse-128" => fn -> {_parser, [_msg]} = Neato.Parser.new() |> Neato.Parser.parse(tcp_packet) end
}, time: 5)

IO.puts "== Publish Throughput"
Benchee.run(%{
  "pub - 128" => fn -> :ok = Neato.pub(pid, "pub128", msg128) end,
}, time: 5)

IO.puts "== Subscribe -> Publish -> Receive Throughput"
Benchee.run(%{
  "subpub-16" => fn ->
    rand = :crypto.strong_rand_bytes(4) |> Base.encode64
    :ok = Neato.sub(pid, self(), rand)
    :ok = Neato.pub(pid, rand, msg16)
    receive do
      {:msg, ^rand, ^msg16} -> :ok
      after 100 -> raise "timed out on sub"
    end
  end,
}, time: 10)

Neato.stop(pid)
