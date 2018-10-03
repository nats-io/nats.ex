# State transitions:
#  :waiting_for_message => receive PING, send PONG => :waiting_for_message
#  :waiting_for_message => receive MSG... -> :waiting_for_message

defmodule Gnat do
  use GenServer
  require Logger
  alias Gnat.{Command, Parser}

  @type message :: %{topic: String.t, body: String.t, reply_to: String.t}

  @default_connection_settings %{
    host: 'localhost',
    port: 4222,
    tcp_opts: [:binary],
    connection_timeout: 3_000,
    ssl_opts: [],
    tls: false,
  }

  @request_sid 0

  @doc """
  Starts a connection to a nats broker

  ```
  {:ok, gnat} = Gnat.start_link(%{host: '127.0.0.1', port: 4222})
  # if the server requires TLS you can start a connection with:
  {:ok, gnat} = Gnat.start_link(%{host: '127.0.0.1', port: 4222, tls: true})
  # if the server requires TLS and a client certificate you can start a connection with:
  {:ok, gnat} = Gnat.start_link(%{tls: true, ssl_opts: [certfile: "client-cert.pem", keyfile: "client-key.pem"]})
  ```

  You can also pass arbitrary SSL or TCP options in the `tcp_opts` and `ssl_opts` keys.
  If you pass custom TCP options please include `:binary`. Gnat uses binary matching to parse messages.

  The final `opts` argument will be passed to the `GenServer.start_link` call so you can pass things like `[name: :gnat_connection]`.
  """
  @spec start_link(map(), keyword()) :: GenServer.on_start
  def start_link(connection_settings \\ %{}, opts \\ []) do
    GenServer.start_link(__MODULE__, connection_settings, opts)
  end

  @doc """
  Gracefull shuts down a connection

  ```
  {:ok, gnat} = Gnat.start_link()
  :ok = Gnat.stop(gnat)
  ```
  """
  @spec stop(GenServer.server) :: :ok
  def stop(pid), do: GenServer.call(pid, :stop)

  @doc """
  Subscribe to a topic

  Supported options:
    * queue_group: a string that identifies which queue group you want to join

  By default each subscriber will receive a copy of every message on the topic.
  When a queue_group is supplied messages will be spread among the subscribers
  in the same group. (see [nats queueing](https://nats.io/documentation/concepts/nats-queueing/))

  ```
  {:ok, gnat} = Gnat.start_link()
  {:ok, subscription} = Gnat.sub(gnat, self(), "topic")
  receive do
    {:msg, %{topic: "topic", body: body}} ->
      IO.puts "Received: \#\{body\}"
  end
  ```
  """
  @spec sub(GenServer.server, pid(), String.t, keyword()) :: {:ok, non_neg_integer()} | {:ok, String.t} | {:error, String.t}
  def sub(pid, subscriber, topic, opts \\ []), do: GenServer.call(pid, {:sub, subscriber, topic, opts})

  @doc """
  Publish a message

  ```
  {:ok, gnat} = Gnat.start_link()
  :ok = Gnat.pub(gnat, "characters", "Ron Swanson")
  ```

  If you want to provide a reply address to receive a response you can pass it as an option.
  [See request-response pattern](http://nats.io/documentation/concepts/nats-req-rep/).

  ```
  {:ok, gnat} = Gnat.start_link()
  :ok = Gnat.pub(gnat, "characters", "Star Lord", reply_to: "me")
  ```
  """
  @spec pub(GenServer.server, String.t, binary(), keyword()) :: :ok
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
  @spec request(GenServer.server, String.t, binary(), keyword()) :: {:ok, message} | {:error, :timeout}
  def request(pid, topic, body, opts \\ []) do
    receive_timeout = Keyword.get(opts, :receive_timeout, 60_000)
    {:ok, subscription} = GenServer.call(pid, {:request, %{recipient: self(), body: body, topic: topic}})
    response = receive do
      {:msg, %{topic: ^subscription}=msg} -> {:ok, msg}
      after receive_timeout ->
        {:error, :timeout}
    end
    :ok = unsub(pid, subscription)
    response
  end

  @doc """
  Unsubscribe from a topic

  Supported options:
    * max_messages: number of messages to be received before automatically unsubscribed

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
  @spec unsub(GenServer.server, non_neg_integer() | String.t, keyword()) :: :ok
  def unsub(pid, sid, opts \\ []), do: GenServer.call(pid, {:unsub, sid, opts})

  @doc """
  Ping the NATS server

  This correlates to the [PING](http://nats.io/documentation/internals/nats-protocol/#PINGPONG) command in the NATS protocol.
  If the NATS server responds with a PONG message this function will return `:ok`
  ```
  {:ok, gnat} = Gnat.start_link()
  :ok = Gnat.ping(gnat)
  ```
  """
  def ping(pid) do
    GenServer.call(pid, {:ping, self()})
    receive do
      :pong -> :ok
    after
      3_000 -> {:error, "No PONG response after 3 sec"}
    end
  end

  @doc "get the number of active subscriptions"
  @spec active_subscriptions(GenServer.server) :: {:ok, non_neg_integer()}
  def active_subscriptions(pid) do
    GenServer.call(pid, :active_subscriptions)
  end

  @impl GenServer
  def init(connection_settings) do
    connection_settings = Map.merge(@default_connection_settings, connection_settings)
    case Gnat.Handshake.connect(connection_settings) do
      {:ok, socket} ->
        parser = Parser.new
        state = %{socket: socket,
                  connection_settings: connection_settings,
                  next_sid: 1,
                  receivers: %{},
                  parser: parser,
                  request_receivers: %{},
                  request_inbox_prefix: "_INBOX.#{nuid()}."}

        state = create_request_subscription(state)
        {:ok, state}
      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_info({:tcp, socket, data}, %{socket: socket}=state) do
    data_packets = receive_additional_tcp_data(socket, [data], 10)
    new_state = Enum.reduce(data_packets, state, fn(data, %{parser: parser}=state) ->
      {new_parser, messages} = Parser.parse(parser, data)
      new_state = %{state | parser: new_parser}
      Enum.reduce(messages, new_state, &process_message/2)
    end)
    {:noreply, new_state}
  end
  def handle_info({:ssl, socket, data}, state) do
    handle_info({:tcp, socket, data}, state)
  end
  def handle_info({:tcp_closed, _}, state) do
    {:stop, "connection closed", state}
  end
  def handle_info({:ssl_closed, _}, state) do
    {:stop, "connection closed", state}
  end
  def handle_info({:tcp_error, _, reason}, state) do
    {:stop, "tcp transport error #{inspect(reason)}", state}
  end
  def handle_info(other, state) do
    Logger.error "#{__MODULE__} received unexpected message: #{inspect other}"
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:stop, _from, state) do
    socket_close(state)
    {:stop, :normal, :ok, state}
  end
  def handle_call({:sub, receiver, topic, opts}, _from, %{next_sid: sid}=state) do
    sub = Command.build(:sub, topic, sid, opts)
    :ok = socket_write(state, sub)
    next_state = add_subscription_to_state(state, sid, receiver) |> Map.put(:next_sid, sid + 1)
    {:reply, {:ok, sid}, next_state}
  end
  def handle_call({:pub, topic, message, opts}, from, state) do
    commands = [Command.build(:pub, topic, message, opts)]
    froms = [from]
    {commands, froms} = receive_additional_pubs(commands, froms, 10)
    :ok = socket_write(state, commands)
    Enum.each(froms, fn(from) -> GenServer.reply(from, :ok) end)
    {:noreply, state}
  end
  def handle_call({:request, request}, _from, state) do
    inbox = make_new_inbox(state)
    new_state = %{state | request_receivers: Map.put(state.request_receivers, inbox, request.recipient)}
    pub = Command.build(:pub, request.topic, request.body, reply_to: inbox)
    :ok = socket_write(new_state, [pub])
    {:reply, {:ok, inbox}, new_state}
  end
  # When the SID is a string, it's a topic, which is used as a key in the request receiver map.
  def handle_call({:unsub, topic, _opts}, _from, state) when is_binary(topic) do
    if Map.has_key?(state.request_receivers, topic) do
      request_receivers = Map.delete(state.request_receivers, topic)
      new_state = %{state | request_receivers: request_receivers}
      {:reply, :ok, new_state}
    else
      {:reply, :ok, state}
    end
  end
  def handle_call({:unsub, sid, opts}, _from, %{receivers: receivers}=state) do
    case Map.has_key?(receivers, sid) do
      false -> {:reply, :ok, state}
      true ->
        command = Command.build(:unsub, sid, opts)
        :ok = socket_write(state, command)
        state = cleanup_subscription_from_state(state, sid, opts)
        {:reply, :ok, state}
    end
  end
  def handle_call({:ping, pinger}, _from, state) do
    :ok = socket_write(state, "PING\r\n")
    {:reply, :ok, Map.put(state, :pinger, pinger)}
  end
  def handle_call(:active_subscriptions, _from, state) do
    active_subscriptions = Enum.count(state.receivers)
    {:reply, {:ok, active_subscriptions}, state}
  end

  defp create_request_subscription(%{request_inbox_prefix: request_inbox_prefix}=state) do
    # Example: "_INBOX.Jhf7AcTGP3x4dAV9.*"
    wildcard_inbox_topic = request_inbox_prefix <> "*"
    sub = Command.build(:sub, wildcard_inbox_topic, @request_sid, [])
    :ok = socket_write(state, [sub])
    add_subscription_to_state(state, @request_sid, self())
  end

  defp make_new_inbox(%{request_inbox_prefix: prefix}), do: prefix <> nuid()

  defp nuid(), do: :crypto.strong_rand_bytes(12) |> Base.encode64

  defp socket_close(%{socket: socket, connection_settings: %{tls: true}}), do: :ssl.close(socket)
  defp socket_close(%{socket: socket}), do: :gen_tcp.close(socket)

  defp socket_write(%{socket: socket, connection_settings: %{tls: true}}, iodata) do
    :ssl.send(socket, iodata)
  end
  defp socket_write(%{socket: socket}, iodata), do: :gen_tcp.send(socket, iodata)

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

  defp process_message({:msg, topic, @request_sid, reply_to, body}, state) do
    if Map.has_key?(state.request_receivers, topic) do
      send state.request_receivers[topic], {:msg, %{topic: topic, body: body, reply_to: reply_to, gnat: self()}}
      state
    else
      Logger.error "#{__MODULE__} got a response for a request, but that is no longer registered"
      state
    end
  end
  defp process_message({:msg, topic, sid, reply_to, body}, state) do
    unless is_nil(state.receivers[sid]) do
      send state.receivers[sid].recipient, {:msg, %{topic: topic, body: body, reply_to: reply_to, gnat: self()}}
      update_subscriptions_after_delivering_message(state, sid)
    else
      Logger.error "#{__MODULE__} got message for sid #{sid}, but that is no longer registered"
      state
    end
  end
  defp process_message(:ping, state) do
    socket_write(state, "PONG\r\n")
    state
  end
  defp process_message(:pong, state) do
    send state.pinger, :pong
    state
  end
  defp process_message({:error, message}, state) do
    :error_logger.error_report([
      type: :gnat_error_from_broker,
      message: message,
    ])
    state
  end

  defp receive_additional_pubs(commands, froms, 0), do: {commands, froms}
  defp receive_additional_pubs(commands, froms, how_many_more) do
    receive do
      {:"$gen_call", from, {:pub, topic, message, opts}} ->
        commands = [Command.build(:pub, topic, message, opts) | commands]
        froms = [from | froms]
        receive_additional_pubs(commands, froms, how_many_more - 1)
    after
      0 -> {commands, froms}
    end
  end

  defp receive_additional_tcp_data(_socket, packets, 0), do: Enum.reverse(packets)
  defp receive_additional_tcp_data(socket, packets, n) do
    receive do
      {:tcp, ^socket, data} ->
        receive_additional_tcp_data(socket, [data | packets], n - 1)
      after
        0 -> Enum.reverse(packets)
    end
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
