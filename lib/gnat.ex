# State transitions:
#  :waiting_for_message => receive PING, send PONG => :waiting_for_message
#  :waiting_for_message => receive MSG... -> :waiting_for_message

defmodule Gnat do
  use GenServer
  require Logger
  alias Gnat.{Command, Parser}

  @default_connection_settings %{
    host: 'localhost',
    port: 4222,
    tcp_opts: [:binary],
  }

  def start_link, do: start_link(%{})
  def start_link(connection_settings) do
    GenServer.start_link(__MODULE__, connection_settings)
  end

  def stop(pid), do: GenServer.call(pid, :stop)

  def sub(pid, subscriber, topic), do: GenServer.call(pid, {:sub, subscriber, topic})

  def pub(pid, topic, message, opts \\ []), do: GenServer.call(pid, {:pub, topic, message, opts})

  @doc """
  Unsubscribe from a topic

  This correlates to the [UNSUB](http://nats.io/documentation/internals/nats-protocol/#UNSUB) command in the nats protocol.
  By default the unsubscribe is affected immediately, but an optional `max_messages` value can be provided which will allow
  `max_messages` to be received before affecting the unsubscribe.
  This is especially useful for [request response](http://nats.io/documentation/concepts/nats-req-rep/) patterns.

  ```
  {:ok, gnat} = Gnat.start_link()
  {:ok, subscription} = Gnat.sub(gnat, self(), "my_inbox")
  :ok = Gnat.unsub(gnat, subscription)
  # OR
  :ok = Gnat.unsub(gnat, subscription, max_messages: 2)
  ```
  """
  def unsub(pid, sid, opts \\ []), do: GenServer.call(pid, {:unsub, sid, opts})

  def init(connection_settings) do
    connection_settings = Map.merge(@default_connection_settings, connection_settings)
    {:ok, tcp} = :gen_tcp.connect(connection_settings.host, connection_settings.port, connection_settings.tcp_opts)
    case perform_handshake(tcp) do
      :ok ->
        parser = Parser.new
        {:ok, %{tcp: tcp, connection_settings: connection_settings, next_sid: 1, receivers: %{}, parser: parser}}
      {:error, reason} ->
        :gen_tcp.close(tcp)
        {:error, reason}
    end
  end

  defp process_message({:msg, topic, sid, reply_to, body}, state) do
    send state.receivers[sid], {:msg, %{topic: topic, body: body, reply_to: reply_to}}
  end
  defp process_message(:ping, state) do
    :gen_tcp.send(state.tcp, "PONG\r\n")
  end
  def handle_info({:tcp, tcp, data}, %{tcp: tcp, parser: parser}=state) do
    Logger.debug "#{__MODULE__} received #{inspect data}"
    {new_parser, messages} = Parser.parse(parser, data)
    Enum.each(messages, &process_message(&1, state))
    {:noreply, %{state | parser: new_parser}}
  end
  def handle_info(other, state) do
    Logger.error "#{__MODULE__} received unexpected message: #{inspect other}"
    {:noreply, state}
  end


  def handle_call(:stop, _from, state) do
    :gen_tcp.close(state.tcp)
    {:stop, :normal, :ok, state}
  end
  def handle_call({:sub, receiver, topic}, _from, %{next_sid: sid}=state) do
    :ok = :gen_tcp.send(state.tcp, ["SUB ", topic, " #{sid}\r\n"])
    receivers = Map.put(state.receivers, sid, receiver)
    next_state = Map.merge(state, %{receivers: receivers, next_sid: sid + 1})
    {:reply, {:ok, sid}, next_state}
  end
  def handle_call({:pub, topic, message, opts}, _from, state) do
    command = Command.build(:pub, topic, message, opts)
    :ok = :gen_tcp.send(state.tcp, command)
    {:reply, :ok, state}
  end
  def handle_call({:unsub, sid, opts}, _from, state) do
    command = Command.build(:unsub, sid, opts)
    :ok = :gen_tcp.send(state.tcp, command)
    {:reply, :ok, state}
  end

  defp perform_handshake(tcp) do
    receive do
      {:tcp, ^tcp, "INFO"<>_} ->
        :gen_tcp.send(tcp, "CONNECT {\"verbose\": false}\r\n")
      after 1000 ->
        {:error, "timed out waiting for info"}
    end
  end
end
