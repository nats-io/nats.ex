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

  @doc """
  Subscribe to a topic

  By default each subscriber will receive a copy of every message on the topic.
  When a queue_group is supplied messages will be spread among the subscribers
  in the same group. (see [nats queueing](https://nats.io/documentation/concepts/nats-queueing/))

  Supported options:
    * queue_group: a string that identifies which queue group you want to join

  ```
  {:ok, gnat} = Gnat.start_link()
  {:ok, subscription} = Gnat.sub(gnat, self(), "topic")
  receive do
    {:msg, %{topic: "topic", body: body}} ->
      IO.puts "Received: \#\{body\}"
  end
  ```
  """
  def sub(pid, subscriber, topic, opts \\ []), do: GenServer.call(pid, {:sub, subscriber, topic, opts})

  def pub(pid, topic, message, opts \\ []), do: GenServer.call(pid, {:pub, topic, message, opts})

  @doc """
  Send a request and listen for a response synchronously

  Following the nats [request-response pattern](http://nats.io/documentation/concepts/nats-req-rep/) this
  function generates a one-time topic to receive replies and then sends a message to the provided topic.

  Supported options:
    * receive_timeout: an integer number of milliseconds to wait for a response. Defaults to 60_000

  ```
  {:ok, gnat} = Gnat.start_link()
  case Gnat.request("i_can_haz_cheezburger", "plZZZZ?!?!?") do
    {:ok, %{body: delicious_cheezburger}} -> :yum
    {:error, :timeout} -> :sad_cat
  end
  ```
  """
  def request(pid, topic, body, opts \\ []) do
    receive_timeout = Keyword.get(opts, :receive_timeout, 60_000)
    inbox = "INBOX-#{:crypto.strong_rand_bytes(12) |> Base.encode64}"
    {:ok, subscription} = GenServer.call(pid, {:request, %{recipient: self(), inbox: inbox, body: body, topic: topic}})
    receive do
      {:msg, %{topic: ^inbox}=msg} -> {:ok, msg}
      after receive_timeout ->
        :ok = unsub(pid, subscription)
        {:error, :timeout}
    end
  end

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

  def handle_info({:tcp, tcp, data}, %{tcp: tcp, parser: parser}=state) do
    Logger.debug "#{__MODULE__} received #{inspect data}"
    {new_parser, messages} = Parser.parse(parser, data)
    new_state = %{state | parser: new_parser}
    new_state = Enum.reduce(messages, new_state, &process_message/2)
    {:noreply, new_state}
  end
  def handle_info(other, state) do
    Logger.error "#{__MODULE__} received unexpected message: #{inspect other}"
    {:noreply, state}
  end


  def handle_call(:stop, _from, state) do
    :gen_tcp.close(state.tcp)
    {:stop, :normal, :ok, state}
  end
  def handle_call({:sub, receiver, topic, opts}, _from, %{next_sid: sid}=state) do
    sub = Command.build(:sub, topic, sid, opts)
    :ok = :gen_tcp.send(state.tcp, sub)
    next_state = add_subscription_to_state(state, sid, receiver) |> Map.put(:next_sid, sid + 1)
    {:reply, {:ok, sid}, next_state}
  end
  def handle_call({:pub, topic, message, opts}, _from, state) do
    command = Command.build(:pub, topic, message, opts)
    :ok = :gen_tcp.send(state.tcp, command)
    {:reply, :ok, state}
  end
  def handle_call({:request, request}, _from, %{next_sid: sid}=state) do
    sub = Command.build(:sub, request.inbox, sid, [])
    unsub = Command.build(:unsub, sid, [max_messages: 1])
    pub = Command.build(:pub, request.topic, request.body, reply_to: request.inbox)
    :ok = :gen_tcp.send(state.tcp, [sub, unsub, pub])
    state = add_subscription_to_state(state, sid, request.recipient) |> cleanup_subscription_from_state(sid, max_messages: 1)
    next_sid = sid + 1
    {:reply, {:ok, sid}, %{state | next_sid: next_sid}}
  end
  def handle_call({:unsub, sid, opts}, _from, state) do
    command = Command.build(:unsub, sid, opts)
    :ok = :gen_tcp.send(state.tcp, command)
    {:reply, :ok, state}
  end

  defp perform_handshake(tcp) do
    receive do
      {:tcp, ^tcp, operation} ->
        "INFO" = operation |> String.split |> List.first |> String.upcase
        :gen_tcp.send(tcp, "CONNECT {\"verbose\": false}\r\n")
      after 1000 ->
        {:error, "timed out waiting for info"}
    end
  end

  defp add_subscription_to_state(%{receivers: receivers}=state, sid, pid) do
    receivers = Map.put(receivers, sid, %{recipient: pid, unsub_after: :infinity})
    %{state | receivers: receivers}
  end

  defp cleanup_subscription_from_state(%{receivers: receivers}=state, sid, []) do
    receivers = Map.delete(receivers, sid)
    %{state | receivers: receivers}
  end
  defp cleanup_subscription_from_state(%{receivers: receivers}=state, sid, [max_messages: n]) do
    receivers = put_in(receivers, [sid, :unsub_after], n)
    %{state | receivers: receivers}
  end

  defp process_message({:msg, topic, sid, reply_to, body}, state) do
    send state.receivers[sid].recipient, {:msg, %{topic: topic, body: body, reply_to: reply_to}}
    update_subscriptions_after_delivering_message(state, sid)
  end
  defp process_message(:ping, state) do
    :gen_tcp.send(state.tcp, "PONG\r\n")
    state
  end

  defp update_subscriptions_after_delivering_message(%{receivers: receivers}=state, sid) do
    receivers = case get_in(receivers, [sid, :unsub_after]) do
                  :infinity -> receivers
                  1 -> Map.delete(receivers, sid)
                  n -> put_in(receivers, [sid, :unsub_after], n - 1)
                end
    %{state | receivers: receivers}
  end
end
