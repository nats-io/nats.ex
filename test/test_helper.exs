ExUnit.configure(exclude: [:pending, :property, :multi_server])

ExUnit.start()

# set assert_receive default timeout
Application.put_env(:ex_unit, :assert_receive_timeout, 1_000)

# cleanup any streams left over by previous test runs
{:ok, conn} = Gnat.start_link(%{}, [name: :jstest])
{:ok, %{streams: streams}} = Gnat.Jetstream.API.Stream.list(conn)
streams = streams || []

Enum.each(streams, fn stream ->
  :ok = Gnat.Jetstream.API.Stream.delete(conn, stream)
end)

:ok = Gnat.stop(conn)


case :gen_tcp.connect('localhost', 4222, [:binary]) do
  {:ok, socket} ->
    :gen_tcp.close(socket)
  {:error, reason} ->
    Mix.raise "Cannot connect to nats-server" <>
              " (http://localhost:4222):" <>
              " #{:inet.format_error(reason)}\n" <>
              "You probably need to start nats-server."
end

# this is used by some property tests, see test/gnat_property_test.exs
Gnat.start_link(%{}, [name: :test_connection])

defmodule RpcEndpoint do
  def init do
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "rpc.>")
    loop(pid)
  end

  def loop(pid) do
    receive do
      {:msg, %{body: body, reply_to: topic}} ->
        Gnat.pub(pid, topic, body)
        loop(pid)
    end
  end
end
spawn(&RpcEndpoint.init/0)

defmodule ExampleService do
  use Gnat.Services.Server

  def request(%{topic: "calc.add", body: body}, _endpoint, _group) when body == "foo" do
    :timer.sleep(10)

    {:reply, "6"}
  end

  def request(%{body: body}, "sub", "calc") when body == "foo" do
    # want some processing time to show up
    :timer.sleep(10)
    {:reply, "4"}
  end

  def request(_, _, _) do
    {:error, "oops"}
  end

  def error(_msg, "oops") do
    {:reply, "500 error"}
  end
end

defmodule ExampleServer do
  use Gnat.Server

  def request(%{topic: "example.good", body: body}) do
    {:reply, "Re: #{body}"}
  end

  def request(%{topic: "example.error"}) do
    {:error, "oops"}
  end

  def request(%{topic: "example.raise"}) do
    raise "oops"
  end

  def error(_msg, "oops") do
    {:reply, "400 error"}
  end

  def error(_msg, %RuntimeError{message: "oops"}) do
    {:reply, "500 error"}
  end

  def error(msg, other) do
    require Logger
    Logger.error("#{msg.topic} failed #{inspect(other)}")
  end
end

{:ok, _pid} = Gnat.ConsumerSupervisor.start_link(%{
  connection_name: :test_connection,
  module: ExampleServer,
  subscription_topics: [
    %{topic: "example.*"}
  ]
})

{:ok , _pid} = Gnat.ConsumerSupervisor.start_link(%{
  connection_name: :test_connection,
  module: ExampleService,
  service_definition: %{
    name: "exampleservice",
    description: "This is an example service",
    version: "0.1.0",
    endpoints: [
      %{
        name: "add",
        group_name: "calc",
      },
      %{
        name: "sub",
        group_name: "calc"
      }
    ]
  }
})

defmodule CheckForExpectedNatsServers do
  def check(tags) do
    check_for_default()
    Enum.each(tags, &check_for_tag/1)
  end

  def check_for_default do
    case :gen_tcp.connect('localhost', 4222, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to nats-server" <>
                  " (tcp://localhost:4222):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start nats-server."
    end
  end

  def check_for_tag(:multi_server) do
    case :gen_tcp.connect('localhost', 4223, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to nats-server" <>
                  " (tcp://localhost:4223):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a nats-server " <>
                  "server that requires authentication with " <>
                  "the following command `nats-server -p 4223 " <>
                  "--user bob --pass alice`."
    end

    case :gen_tcp.connect('localhost', 4224, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to nats-server" <>
                  " (tcp://localhost:4224):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a nats-server " <>
                  "server that requires tls with " <>
                  "a command like `nats-server -p 4224 " <>
                  "--tls --tlscert test/fixtures/server-cert.pem " <>
                  "--tlskey test/fixtures/server-key.pem`."
    end

    case :gen_tcp.connect('localhost', 4225, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to nats-server" <>
                  " (tcp://localhost:4225):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a nats-server " <>
                  "server that requires tls with " <>
                  "a command like `nats-server -p 4225 --tls " <>
                  "--tlscert test/fixtures/server-cert.pem " <>
                  "--tlskey test/fixtures/server-key.pem " <>
                  "--tlscacert test/fixtures/ca.pem --tlsverify"
    end

    case :gen_tcp.connect('localhost', 4226, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to nats-server" <>
                  " (tcp://localhost:4226):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a nats-server " <>
                  "server that requires authentication with " <>
                  "the following command `nats-server -p 4226 " <>
                  "--auth SpecialToken`."
    end

    case :gen_tcp.connect('localhost', 4227, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to nats-server" <>
                  " (tcp://localhost:4227):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a nats-server " <>
                  "server that requires authentication with " <>
                  "the following command `nats-server -p 4227 " <>
                  "-c test/fixtures/nkey_config`."
    end
  end
  def check_for_tag(_), do: :ok
end
