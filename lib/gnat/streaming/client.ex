defmodule Gnat.Streaming.Client do
  use GenServer
  require Logger
  alias Gnat.Streaming.Protocol

  def start_link(settings, options \\ []) do
    GenServer.start_link(__MODULE__, settings, options)
  end

  @spec pub(GenServer.server, String.t, binary(), keyword()) :: :ok | {:error, term()}
  def pub(streaming_client, subject, payload, options \\ []) do
    reply_to = Keyword.get(options, :reply_to)
    guid = Keyword.get(options, :guid) || nuid()
    with {:ok, client_id, pub_prefix, connection_pid} <- GenServer.call(streaming_client, :pub_info),
         pub_subject <- "#{pub_prefix}.me",
         pub_msg <- encode_pub_msg(subject, payload, guid, reply_to, client_id),
         {:ok, %{body: pb}} <- Gnat.request(connection_pid, pub_subject, pub_msg) do
      case Protocol.PubAck.decode(pb) do
        %Protocol.PubAck{error: ""} -> :ok
        %Protocol.PubAck{error: err} -> {:error, err}
      end
    end
  end

  @impl GenServer
  def init(settings) do
    Process.flag(:trap_exit, true)
    send self(), :connect
    state = %{
      client_id: Map.fetch!(settings, :client_id),
      conn_id: Map.get(settings, :conn_id) || nuid(),
      connection_name: Map.fetch!(settings, :connection_name),
      connection_pid: nil,
      connect_response: nil,
      status: :disconnected,
    }
    {:ok, state}
  end

  @impl GenServer
  def handle_call(:pub_info, _from, %{status: :connected}=state) do
    pub_info = {:ok, state.client_id, state.connect_response.pubPrefix, state.connection_pid}
    {:reply, pub_info, state}
  end

  @impl GenServer
  def handle_info(:connect, %{connection_name: name}=state) do
    case Process.whereis(name) do
      nil ->
        Process.send_after(self(), :connect, 2_000)
        {:noreply, state}
      connection_pid ->
        _ref = Process.monitor(connection_pid)
        heartbeat_subject = "#{state.client_id}.#{state.conn_id}.heartbeat"
        req = Protocol.ConnectRequest.new(clientID: state.client_id, connID: state.conn_id, heartbeatInbox: heartbeat_subject) |> Protocol.ConnectRequest.encode()
        {:ok, _sid} = Gnat.sub(connection_pid, self(), heartbeat_subject)
        case Gnat.request(connection_pid, "_STAN.discover.test-cluster", req) do
          {:ok, %{body: msg}} ->
            msg = Protocol.ConnectResponse.decode(msg) |> IO.inspect
            {:noreply, %{state | status: :connected, connection_pid: connection_pid, connect_response: msg}}
          {:error, reason} ->
            Process.send_after(self(), :connect, 2_000)
            Logger.error("Failed to connect to NATS Streaming server: #{inspect(reason)}")
            {:noreply, state}
        end
    end
  end
  # receive heartbeat messages and respond
  def handle_info({:msg, %{body: "", reply_to: reply_to}}, state) do
    Gnat.pub(state.connection_pid, reply_to, "")
    {:noreply, state}
  end
  # the connection has died, we need to reset our state and try to reconnect
  def handle_info({:DOWN, _ref, :process, connection_pid, reason}, %{connection_pid: connection_pid}=state) do
    Logger.info("connection down #{inspect(reason)}")
    Process.send_after(self(), :connect, 2_000)
    {:noreply, %{state | status: :disconnected, connection_pid: nil, connect_response: nil}}
  end
  # Ignore DOWN and task result messages from the spawned tasks
  def handle_info({:DOWN, _ref, :process, _task_pid, _reason}, state), do: {:noreply, state}
  def handle_info({ref, _result}, state) when is_reference(ref), do: {:noreply, state}

  def handle_info(other, state) do
    Logger.error "#{__MODULE__} received unexpected message #{inspect other}"
    {:noreply, state}
  end

  @impl GenServer
  def terminate(:shutdown, _state) do
    # TODO Send CloseRequest https://nats.io/documentation/streaming/nats-streaming-protocol/#CLOSEREQ
  end
  def terminate(reason, _state) do
    Logger.error "#{__MODULE__} unexpected shutdown #{inspect reason}"
  end

  defp encode_pub_msg(subject, payload, guid, reply_to, client_id) do
    Protocol.PubMsg.new(
      clientID: client_id,
      guid: guid,
      subject: subject,
      data: payload,
      reply: reply_to
    ) |> Protocol.PubMsg.encode()
  end

  defp nuid, do: :crypto.strong_rand_bytes(12) |> Base.encode64
end
