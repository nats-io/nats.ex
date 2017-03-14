# State transitions:
#  :waiting_for_message => receive PING, send PONG => :waiting_for_message
#  :waiting_for_message => receive MSG... -> :waiting_for_message

defmodule Neato do
  use GenServer
  require Logger

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

  def init(connection_settings) do
    connection_settings = Map.merge(@default_connection_settings, connection_settings)
    {:ok, tcp} = :gen_tcp.connect(connection_settings.host, connection_settings.port, connection_settings.tcp_opts)
    case perform_handshake(tcp) do
      :ok ->
        {:ok, %{tcp: tcp, connection_settings: connection_settings, state: :waiting_for_message, next_sid: 1, receivers: %{}}}
      {:error, reason} ->
        :gen_tcp.close(tcp)
        {:error, reason}
    end
  end

  def handle_info({:tcp, tcp, "MSG "<>rest}, %{tcp: tcp, state: :waiting_for_message}=state) do
    [cmd | msg_chunks] = String.split(rest, "\r\n")
    [topic, sid_str, size_str] = String.split(cmd, " ")
    msg = Enum.join(msg_chunks, "\r\n") |> String.strip
    sid = String.to_integer(sid_str)
    send state.receivers[sid], {:msg, topic, msg}
    {:noreply, state}
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
    {:reply, :ok, next_state}
  end
  def handle_call({:pub, topic, message}, _from, state) do
    publish_data = [["PUB ", topic, " #{IO.iodata_length(message)}\r\n"], [message, "\r\n"]]
    :ok = :gen_tcp.send(state.tcp, publish_data)
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
