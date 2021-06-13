msg1024 = :crypto.strong_rand_bytes(1024)
msg128  = :crypto.strong_rand_bytes(128)
msg16   = :crypto.strong_rand_bytes(16)

inputs = %{
  "16 byte" => "MSG topic 1 16\r\n#{msg16}\r\n",
  "128 byte" => "MSG topic 1 128\r\n#{msg128}\r\n",
  "1024 byte" => "MSG topic 1 1024\r\n#{msg1024}\r\n",
  "7 byte with headers" => "HMSG SUBJECT 1 REPLY 48 55\r\nNATS/1.0\r\nHeader1: X\r\nHeader1: Y\r\nHeader2: Z\r\n\r\nPAYLOAD\r\n"
}

parsec = Gnat.Parsec.new()
Benchee.run(%{
  "parsec" => fn(tcp_packet) -> {_parse, [_msg]} = Gnat.Parsec.parse(parsec, tcp_packet) end,
}, time: 10, parallel: 1, inputs: inputs, formatters: [{Benchee.Formatters.Console, comparisons: false}])
