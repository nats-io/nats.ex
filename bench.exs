{:ok, pid} = Neato.start_link(%{host: '127.0.0.1', port: 4222})

msg128="74c93e71c5aa03ad4f0881caa374ba1af08f3e4a04ce5f8bd0b2d82d6d72de6eef3e46ed8d8c3dbe24d0f6109115dcdf13280d1c13c2f6d22d14336b29df8e65"
Benchee.run(%{
  "pub - 128" => fn -> :ok = Neato.pub(pid, "pub128", msg128) end,
  "pubsub - 128" => fn ->
    rand = :crypto.strong_rand_bytes(4) |> Base.encode64
    :ok = Neato.sub(pid, self(), rand)
    :ok = Neato.pub(pid, rand, msg128)
    receive do
      {:msg, ^rand, ^msg128} -> :ok
      after 100 -> raise "timed out on sub"
    end
  end,
}, time: 5)

Neato.stop(pid)
