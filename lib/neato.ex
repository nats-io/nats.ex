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

  def init(connection_settings) do
    connection_settings = Map.merge(@default_connection_settings, connection_settings)
    {:ok, tcp} = :gen_tcp.connect(connection_settings.host, connection_settings.port, connection_settings.tcp_opts)
    case perform_handshake(tcp, :waiting_for_info) do
      :ok ->
        {:ok, %{tcp: tcp, connection_settings: connection_settings, state: :waiting_for_message}}
      {:error, reason} ->
        :gen_tcp.close(tcp)
        {:error, reason}
    end
  end

  def handle_info({:tcp, tcp, "MSG"<>rest}, %{tcp: tcp, state: :waiting_for_message}=state) do
    Logger.info "#{__MODULE__} received: MSG#{rest}"
    {:noreply, state}
  end

  def handle_call(:stop, _from, state) do
    :gen_tcp.close(state.tcp)
    {:stop, :normal, :ok, state}
  end

  defp perform_handshake(tcp, :waiting_for_info) do
    receive do
      {:tcp, ^tcp, "INFO"<>_} ->
        :gen_tcp.send(tcp, "CONNECT {}\r\n")
        perform_handshake(tcp, :waiting_for_ack)
      after 1000 ->
        {:error, "timed out waiting for info"}
    end
  end
  defp perform_handshake(tcp, :waiting_for_ack) do
    receive do
      {:tcp, ^tcp, "+OK\r\n"} -> :ok
      after 1000 ->
        {:error, "timed out waiting for connection ack"}
    end
  end
end
