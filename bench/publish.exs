inputs = %{
  "16 byte" => :crypto.strong_rand_bytes(16),
  "128 byte" => :crypto.strong_rand_bytes(128),
  "1024 byte" => :crypto.strong_rand_bytes(1024),
}

{:ok, client_pid} = Gnat.start_link(%{host: '127.0.0.1', port: 4222})

Benchee.run(%{
  "pub" => fn(msg) -> :ok = Gnat.pub(client_pid, "echo", msg) end,
}, time: 10, parallel: 1, console: [comparison: false], inputs: inputs)
