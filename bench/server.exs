num_connections = 4
num_subscribers = 4

Enum.each(0..(num_connections - 1), fn(i) ->
  name = :"gnat#{i}"
  {:ok, _pid} = Gnat.start_link(%{}, name: name)
end)

Enum.each(0..(num_subscribers - 1), fn(i) ->
  name = :"consumer#{i}"
  conn_name = :"gnat#{rem(i, num_connections)}"
  IO.puts "#{name} will use #{conn_name}"
  {:ok, _pid} = Gnat.ConsumerSupervisor.start_link(%{connection_name: conn_name, consuming_function: {EchoServer, :handle}, subscription_topics: [%{topic: "echo", queue_group: "echo"}]})
end)

defmodule EchoServer do
  def handle(%{body: body, reply_to: reply_to, gnat: gnat_pid}) do
    Gnat.pub(gnat_pid, reply_to, body)
  end

  def wait_loop do
    :timer.sleep(1_000)
    wait_loop()
  end
end

EchoServer.wait_loop()
