# State transitions:
#  :waiting_for_message => receive PING, send PONG => :waiting_for_message
#  :waiting_for_message => receive MSG... -> :waiting_for_message

defmodule Gnat do
  use GenServer
  require Logger
  alias Gnat.Parser

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

  def pub(pid, topic, message), do: GenServer.call(pid, {:pub, topic, message})

  def unsub(pid, sid), do: GenServer.call(pid, {:unsub, sid})

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

  def handle_info({:tcp, tcp, data}, %{tcp: tcp, parser: parser}=state) do
    {new_parser, messages} = Parser.parse(parser, data)
    Enum.each(messages, fn({:msg, topic, sid, body}) ->
      send state.receivers[sid], {:msg, topic, body}
    end)
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
  def handle_call({:pub, topic, message}, _from, state) do
    publish_data = [["PUB ", topic, " #{IO.iodata_length(message)}\r\n"], [message, "\r\n"]]
    :ok = :gen_tcp.send(state.tcp, publish_data)
    {:reply, :ok, state}
  end
  def handle_call({:unsub, sid}, _from, state) do
    unsub_data = ["UNSUB ", Integer.to_string(sid), "\r\n"]
    :ok = :gen_tcp.send(state.tcp, unsub_data)
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

  defp wait_for_ack(tcp, timeout) do
    receive do
      {:tcp, ^tcp, "+OK\r\n"} -> :ok
      after timeout -> {:error, :timeout}
    end
  end
end
