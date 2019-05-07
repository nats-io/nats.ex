defmodule Gnat.Streaming.Client do
  @behaviour :gen_statem

  @enforce_keys [:client_id, :conn_id, :connection_name]
  defstruct client_id: nil,
            conn_id: nil,
            connection_name: nil,
            connection_pid: nil,
            close_subject: nil,
            heartbeat_subject: nil,
            pub_subject: nil,
            sub_subject: nil,
            unsub_subject: nil

  @type t :: %__MODULE__{
    client_id: String.t,
    conn_id: String.t,
    connection_name: atom(),
    connection_pid: pid() | nil,
    close_subject: String.t | nil,
    pub_subject: String.t | nil,
    sub_subject: String.t | nil,
    unsub_subject: String.t | nil
  }

  require Logger
  alias Gnat.Streaming.Protocol

  def start_link(settings, options \\ []) do
    :gen_statem.start_link(__MODULE__, settings, options)
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

  # Callback Functions

  @impl :gen_statem
  def callback_mode(), do: :state_functions

  @impl :gen_statem
  def init(settings) do
    Process.flag(:trap_exit, true)
    state = new(settings)
    {:ok, :disconnected, state, [{:next_event, :internal, :connect}]}
  end

  @impl :gen_statem
  def terminate(:shutdown, _state, _data) do
    Logger.error "#{__MODULE__} TODO - I should send a CloseRequest to notify the broker that I'm going away"
    # TODO Send CloseRequest https://nats.io/documentation/streaming/nats-streaming-protocol/#CLOSEREQ
  end
  def terminate(reason, _state, _data) do
    Logger.error "#{__MODULE__} unexpected shutdown #{inspect reason}"
  end

  # Internal State Functions

  @doc false
  def new(settings) do
    connection_name = Keyword.fetch!(settings, :connection_name)
    client_id = Keyword.get(settings, :client_id) || nuid()
    conn_id = Keyword.get(settings, :conn_id) || nuid()
    heartbeat_subject = "#{client_id}.#{conn_id}.heartbeat"
    %__MODULE__{client_id: client_id, conn_id: conn_id, connection_name: connection_name, heartbeat_subject: heartbeat_subject}
  end

  @doc false
  def disconnected(:internal, :connect, %__MODULE__{connection_name: connection_name}) do
    maybe_pid = Process.whereis(connection_name)
    {:keep_state_and_data, [{:next_event, :internal, {:find_connection, maybe_pid}}]}
  end
  def disconnected(:timeout, :reconnect, state), do: disconnected(:internal, :connect, state)
  def disconnected(:internal, {:find_connection, nil}, _state) do
    {:keep_state_and_data, [{:timeout, 250, :reconnect}]}
  end
  def disconnected(:internal, {:find_connection, pid}, state) when is_pid(pid) do
    state = %__MODULE__{state | connection_pid: pid}
    actions = [{:next_event, :internal, :monitor_and_subscribe}]
    {:next_state, :connected, state, actions}
  end

  @doc false
  def connected(:internal, :monitor_and_subscribe, %__MODULE__{} = state) do
    _ref = Process.monitor(state.connection_pid)
    {:ok, _sid} = Gnat.sub(state.connection_pid, self(), state.heartbeat_subject)
    actions = [{:next_event, :internal, :register}]
    {:keep_state_and_data, actions}
  end
  def connected(:internal, :register, %__MODULE__{} = state) do
    req = Protocol.ConnectRequest.new(
      clientID: state.client_id,
      connID: state.conn_id,
      heartbeatInbox: state.heartbeat_subject
    ) |> Protocol.ConnectRequest.encode()

    case Gnat.request(state.connection_pid, "_STAN.discover.test-cluster", req) do
      {:ok, %{body: msg}} ->
        msg = Protocol.ConnectResponse.decode(msg) |> IO.inspect
        actions = [{:next_event, :internal, {:connect_response, msg}}]
        {:keep_state_and_data, actions}
      {:error, reason} ->
        Logger.error("Failed to connect to NATS Streaming server: #{inspect(reason)}")
        actions = [{:timeout, 1_000, :reregister}]
        {:keep_state_and_data, actions}
    end
  end
  def connected(:timeout, :reregister, state), do: connected(:internal, :register, state)
  def connected(:internal, {:connect_response, response}, %__MODULE__{} = state) do
    if response.error == "" do
      state =
        state
        |> Map.put(:close_subject, response.closeRequests)
        |> Map.put(:pub_subject, "#{response.pubPrefix}.#{state.client_id}.#{state.conn_id}")
        |> Map.put(:sub_subject, response.subRequests)
        |> Map.put(:unsub_subject, response.unsubRequests)
      {:next_state, :registered, state, []}
    else
      Logger.error("Got Connection Error From NATS Streaming server #{response.error}")
      {:keep_state_and_data, [{:timeout, 1_000, :reregister}]}
    end
  end
  def connected(:info, {:DOWN, _ref, :process, pid, _reason}, %__MODULE__{connection_pid: pid} = state) do
    state = %__MODULE__{state | connection_pid: nil}
    {:next_state, :disconnected, state, [{:timeout, 250, :reconnect}]}
  end

  @doc false
  def registered({:call, from}, :pub_info, state) do
    pub_info = {state.client_id, state.pub_subject, state.connection_pid}
    {:keep_state_and_data, [{:reply, from, {:ok, pub_info}}]}
  end
  def registered(:info, {:msg, %{body: "", reply_to: reply_to}}, _state) do
    {:keep_state_and_data, [{:next_event, :internal, {:pub, reply_to, ""}}]}
  end
  def registered(:internal, {:pub, subject, body}, %__MODULE__{} = state) do
    :ok = Gnat.pub(state.connection_pid, subject, body)
    {:keep_state_and_data, []}
  end
  def registered(:info, {:DOWN, _ref, :process, pid, _reason}, %__MODULE__{connection_pid: pid} = state) do
    state = %__MODULE__{state | connection_pid: nil, close_subject: nil, pub_subject: nil, sub_subject: nil, unsub_subject: nil}
    {:next_state, :disconnected, state, [{:timeout, 250, :reconnect}]}
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
