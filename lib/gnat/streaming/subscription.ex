defmodule Gnat.Streaming.Subscription do
  @behaviour :gen_statem

  @enforce_keys [:client_name, :consuming_function, :subject, :task_supervisor_pid]
  defstruct ack_subject: nil,
            ack_wait_in_sec: 30,
            client_id: nil,
            client_name: nil,
            consuming_function: nil,
            connection_pid: nil,
            inbox: nil,
            max_in_flight: 100,
            sub_subject: nil,
            subject: nil,
            task_supervisor_pid: nil

  @type t :: %__MODULE__{
    ack_subject: String.t,
    ack_wait_in_sec: non_neg_integer(),
    client_id: String.t | nil,
    client_name: atom(),
    consuming_function: {atom(), atom()},
    connection_pid: pid() | nil,
    inbox: String.t | nil,
    max_in_flight: non_neg_integer(),
    sub_subject: String.t,
    subject: String.t,
    task_supervisor_pid: pid()
  }

  require Logger
  alias Gnat.Streaming.{Client, Protocol}

  def start_link(settings, options \\ []) do
    :gen_statem.start_link(__MODULE__, settings, options)
  end

  # Callback Functions

  @impl :gen_statem
  def callback_mode(), do: :state_functions

  @impl :gen_statem
  def init(settings) do
    Process.flag(:trap_exit, true)
    {:ok, task_supervisor_pid} = Task.Supervisor.start_link()
    state = new(settings, task_supervisor_pid)
    {:ok, :disconnected, state, [{:next_event, :internal, :connect}]}
  end

  @impl :gen_statem
  @spec terminate(any(), any(), any()) :: :ok | {:error, any()}
  def terminate(:shutdown, _state, _data) do
    Logger.error "#{__MODULE__} TODO - I should send an UnsubscribeRequest to notify the broker that I'm going away"
    # TODO Send CloseRequest https://nats.io/documentation/streaming/nats-streaming-protocol/#UNSUBREQ
  end
  def terminate(reason, _state, _data) do
    Logger.error "#{__MODULE__} unexpected shutdown #{inspect(reason)}"
  end

  # Internal State Functions

  @doc false
  def new(settings, task_supervisor_pid) do
    client_name = Keyword.fetch!(settings, :client_name)
    {mod, fun} = Keyword.fetch!(settings, :consuming_function)
    subject = Keyword.fetch!(settings, :subject)
    %__MODULE__{
      client_name: client_name,
      consuming_function: {mod, fun},
      subject: subject,
      task_supervisor_pid: task_supervisor_pid
    }
  end

  @doc false
  def disconnected(:internal, :connect, %__MODULE__{client_name: client_name}) do
    client_info = Client.sub_info(client_name)
    {:keep_state_and_data, [{:next_event, :internal, {:client_info, client_info}}]}
  end
  def disconnected({:timeout, :reconnect}, _, state), do: disconnected(:internal, :connect, state)
  def disconnected(:internal, {:client_info, {:error, _reason}}, _state) do
    {:keep_state_and_data, [{{:timeout, :reconnect}, 250, :reconnect}]}
  end
  def disconnected(:internal, {:client_info, {:ok, client_info}}, %__MODULE__{} = state) do
    {client_id, sub_subject, connection_pid} = client_info
    inbox = "#{client_id}.#{state.subject}.INBOX"
    state = %__MODULE__{state | inbox: inbox, client_id: client_id, connection_pid: connection_pid, sub_subject: sub_subject}
    actions = [{:next_event, :internal, :monitor_and_listen}]
    {:next_state, :connected, state, actions}
  end

  @doc false
  def connected(:internal, :monitor_and_listen, %__MODULE__{} = state) do
    _ref = Process.monitor(state.connection_pid)
    {:ok, _sid} = Gnat.sub(state.connection_pid, self(), state.inbox)
    {:keep_state_and_data, [{:next_event, :internal, :subscribe}]}
  end
  def connected({:timeout, :resubscribe}, _, %__MODULE__{} = state), do: connected(:internal, :subscribe, state)
  def connected(:internal, :subscribe, %__MODULE__{} = state) do
    req = Protocol.SubscriptionRequest.new(
      ackWaitInSecs: state.ack_wait_in_sec,
      clientID: state.client_id,
      inbox: state.inbox,
      maxInFlight: state.max_in_flight,
      subject: state.subject
    ) |> Protocol.SubscriptionRequest.encode()
    case Gnat.request(state.connection_pid, state.sub_subject, req) do
      {:ok, %{body: msg}} ->
        msg = Protocol.SubscriptionResponse.decode(msg)
        actions = [{:next_event, :internal, {:subscription_response, msg}}]
        {:keep_state_and_data, actions}
      {:error, reason} ->
        Logger.error("Failed to subscribe to NATS Streaming server: #{inspect(reason)}")
        actions = [{{:timeout, :resubscribe}, 1_000, :resubscribe}]
        {:keep_state_and_data, actions}
    end
  end
  def connected(:internal, {:subscription_response, %Protocol.SubscriptionResponse{} = response}, %__MODULE__{} = state) do
    if response.error == "" do
      state = %__MODULE__{state | ack_subject: response.ackInbox}
      {:next_state, :subscribed, state, []}
    else
      Logger.error("Failed to subscribe to NATS Streaming server: #{response.error}")
      {:keep_state_and_data, [{{:timeout, :resubscribe}, 1_000, :resubscribe}]}
    end
  end
  def connected(:info, {:DOWN, _ref, :process, pid, _reason}, %__MODULE__{connection_pid: pid} = state) do
    state = %__MODULE__{state | client_id: nil, connection_pid: nil, inbox: nil, sub_subject: nil}
    actions = [{{:timeout, :reconnect}, 250, :reconnect}]
    {:next_state, :disconnected, state, actions}
  end

  @doc false
  def subscribed(:info, {:DOWN, _ref, :process, pid, _reason}, %__MODULE__{connection_pid: pid} = state) do
    state = %__MODULE__{state | ack_subject: nil, client_id: nil, connection_pid: nil, inbox: nil, sub_subject: nil}
    actions = [{{:timeout, :reconnect}, 250, :reconnect}]
    {:next_state, :disconnected, state, actions}
  end
  # ignore down messages for task processes
  def subscribed(:info, {:DOWN, _ref, :process, task_pid, _reason}, _state) do
    Logger.error("DOWN #{inspect(task_pid)}")
    {:keep_state_and_data, []}
  end
  # ignore task finished messages
  def subscribed(:info, {ref, _return_value}, _state) when is_reference(ref) do
    {:keep_state_and_data, []}
  end
  def subscribed(:info, {:msg, %{body: protobuf}}, %__MODULE__{} = state) do
    Task.Supervisor.async_nolink(state.task_supervisor_pid, __MODULE__, :consume_message, [protobuf, state.consuming_function, state.connection_pid, state.ack_subject])

    {:keep_state_and_data, []}
  end

  def consume_message(protobuf, {mod, fun}, connection_pid, ack_subject) do
    pub_msg = Protocol.MsgProto.decode(protobuf)
    message = %Gnat.Streaming.Message{
      ack_subject: ack_subject,
      connection_pid: connection_pid,
      data: pub_msg.data,
      redelivered: pub_msg.redelivered,
      reply: pub_msg.reply,
      sequence: pub_msg.sequence,
      subject: pub_msg.subject,
      timestamp: pub_msg.timestamp
    }
    apply(mod, fun, [message])
  end
end
