defmodule EchoService do
  use Gnat.Services.Server

  def request(%{body: body}, "echo", _group) do
    {:reply, body}
  end

  def definition do
    %{
      name: "echo",
      description: "This is an example service",
      version: "0.0.1",
      endpoints: [
        %{
          name: "echo",
          group_name: "mygroup",
        }
      ]
    }
  end
end

conn_supervisor_settings = %{
  name: :gnat, # (required) the registered named you want to give the Gnat connection
  backoff_period: 1_000, # number of milliseconds to wait between consecutive reconnect attempts (default: 2_000)
  connection_settings: [
    %{host: '127.0.0.1', port: 4222},
  ]
}
{:ok, _pid} = Gnat.ConnectionSupervisor.start_link(conn_supervisor_settings)

# let the connection get established
:timer.sleep(100)

consumer_supervisor_settings = %{
  connection_name: :gnat,
  module: EchoService, # a module that implements the Gnat.Services.Server behaviour
  service_definition: EchoService.definition()
}

{:ok, _pid} = Gnat.ConsumerSupervisor.start_link(consumer_supervisor_settings)

# wait for the connection and consumer to be ready
:timer.sleep(2000)

{:ok, client_pid} = Gnat.start_link(%{host: '127.0.0.1', port: 4222})

inputs = %{
  "16 byte" => :crypto.strong_rand_bytes(16),
  "256 byte" => :crypto.strong_rand_bytes(256),
  "1024 byte" => :crypto.strong_rand_bytes(1024),
}

Benchee.run(%{
  "service" => fn(msg) -> {:ok, %{body: ^msg}} = Gnat.request(client_pid, "mygroup.echo", msg) end,
}, time: 10, parallel: 1, inputs: inputs, formatters: [{Benchee.Formatters.Console, comparisons: false}])
