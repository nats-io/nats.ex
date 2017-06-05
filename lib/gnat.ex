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
    connection_timeout: 3_000,
    ssl_opts: [],
    tls: false,
  }

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

  def init(connection_settings) do
    connection_settings = Map.merge(@default_connection_settings, connection_settings)
    case Gnat.Handshake.connect(connection_settings) do
      {:ok, socket} ->
        parser = Parser.new
        {:ok, %{socket: socket, connection_settings: connection_settings, next_sid: 1, receivers: %{}, parser: parser}}
      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_info({:tcp, socket, data}, %{socket: socket, parser: parser}=state) do
    {new_parser, messages} = Parser.parse(parser, data)
    new_state = %{state | parser: new_parser}
    new_state = Enum.reduce(messages, new_state, &process_message/2)
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
  def handle_call({:pub, topic, message, opts}, _from, state) do
    command = Command.build(:pub, topic, message, opts)
    :ok = socket_write(state, command)
    {:reply, :ok, state}
  end
  def handle_call({:request, request}, _from, %{next_sid: sid}=state) do
    sub = Command.build(:sub, request.inbox, sid, [])
    unsub = Command.build(:unsub, sid, [max_messages: 1])
    pub = Command.build(:pub, request.topic, request.body, reply_to: request.inbox)
    :ok = socket_write(state, [sub, unsub, pub])
    state = add_subscription_to_state(state, sid, request.recipient) |> cleanup_subscription_from_state(sid, max_messages: 1)
    next_sid = sid + 1
    {:reply, {:ok, sid}, %{state | next_sid: next_sid}}
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

  defp process_message({:msg, topic, sid, reply_to, body}, state) do
    unless is_nil(state.receivers[sid]) do
      send state.receivers[sid].recipient, {:msg, %{topic: topic, body: body, reply_to: reply_to}}
      update_subscriptions_after_delivering_message(state, sid)
    else
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

  defp update_subscriptions_after_delivering_message(%{receivers: receivers}=state, sid) do
    receivers = case get_in(receivers, [sid, :unsub_after]) do
                  :infinity -> receivers
                  1 -> Map.delete(receivers, sid)
                  n -> put_in(receivers, [sid, :unsub_after], n - 1)
                end
    %{state | receivers: receivers}
  end
end
