# State transitions:
#  :waiting_for_message => receive PING, send PONG => :waiting_for_message
#  :waiting_for_message => receive MSG... -> :waiting_for_message

defmodule Gnat do
  use GenServer
  require Logger
  alias Gnat.{Command, Parsec}

  @type t :: GenServer.server()
  @type headers :: [{binary(), iodata()}]

  # A message received from NATS will be delivered to your process in this form.
  # Please note that the `:reply_to` and `:headers` keys are optional.
  # They will only be present if the message was received from the NATS server with
  # headers or a reply_to topic
  @type message :: %{
    gnat: t(),
    topic: String.t(),
    body: String.t(),
    sid: non_neg_integer(),
    reply_to: String.t(),
    headers: headers()
  }
  @type sent_message :: {:msg, message()}

  @typedoc """
  * `connection_timeout` - limits how long it can take to establish a connection to a server
  * `host` - The location of the NATS server
  * `port` - The port the NATS server is listening on
  * `ssl_opts` - Options for connecting over SSL
  * `tcp_opts` - Options for connecting over TCP
  * `tls` - If the server should use a TLS connection
  * `inbox_prefix` - Prefix to use for the message inbox of this connection
  * `no_responders` - Enable the no responders behavior (see `Gnat.request/4`)
  """
  @type connection_settings :: %{
    optional(:connection_timeout) => non_neg_integer(),
    optional(:host) => binary(),
    optional(:inbox_prefix) => binary(),
    optional(:port) => non_neg_integer(),
    optional(:ssl_opts) => list(),
    optional(:tcp_opts) => list(),
    optional(:tls) => boolean(),
    optional(:no_responders) => boolean(),
  }

  @typedoc """
  [Info Protocol](https://docs.nats.io/reference/reference-protocols/nats-protocol#info)

  * `client_id` - An optional unsigned integer (64 bits) representing the internal client identifier in the server. This can be used to filter client connections in monitoring, correlate with error logs, etc...
  * `client_ip` - The IP address the client is connecting from
  * `cluster` - The name of the cluster if any
  * `cluster_dynamic` - If the cluster is dynamic
  * `connect_urls` - An optional list of server urls that a client can connect to.
  * `ws_connect_urls` - An optional list of server urls that a websocket client can connect to.
  * `git_commit` - The git commit associated with this NATS version
  * `go` - The version of golang the NATS server was built with
  * `headers` - If messages can have headers in them
  * `host` - The IP address used to start the NATS server, by default this will be 0.0.0.0 and can be configured with -client_advertise host:port
  * `jetstream` - If the server is using JetStream features
  * `max_payload` - Maximum payload size, in bytes, that the server will accept from the client
  * `port` - The port number the NATS server is configured to listen on
  * `proto` - An integer indicating the protocol version of the server. The server version 1.2.0 sets this to 1 to indicate that it supports the "Echo" feature.
  * `server_id` - The unique identifier of the NATS server
  * `server_name` - A name for the server
  * `version` - The version of the NATS server
  * `ldm` - If the server supports Lame Duck Mode notifications, and the current server has transitioned to lame duck, ldm will be set to true.
  * `auth_required` - If this is set, then the client should try to authenticate upon connect.
  * `tls_required` - If this is set, then the client must perform the TLS/1.2 handshake. Note, this used to be ssl_required and has been updated along with the protocol from SSL to TLS.
  * `tls_verify` - If this is set, the client must provide a valid certificate during the TLS handshake.
  * `tls_available` - If the server can use TLS
  """
  @type server_info :: %{
    :client_id => non_neg_integer(),
    :client_ip => binary(),
    optional(:ip) => binary(),
    optional(:cluster) => binary(),
    optional(:cluster_dynamic) => boolean(),
    optional(:connect_urls) => list(binary()),
    optional(:ws_connect_urls) => list(binary()),
    optional(:git_commit) => binary(),
    :go => binary(),
    :headers => boolean(),
    :host => binary(),
    optional(:jetstream) => binary(),
    :max_payload => integer(),
    :port => non_neg_integer(),
    :proto => integer(),
    :server_id => binary(),
    :server_name => binary(),
    :version => binary(),
    optional(:ldm) => boolean(),
    optional(:tls_verify) => boolean(),
    optional(:tls_available) => boolean(),
    optional(:tls_required) => boolean(),
    optional(:auth_required) => boolean(),
  }

  @default_connection_settings %{
    host: 'localhost',
    port: 4222,
    tcp_opts: [:binary],
    connection_timeout: 3_000,
    ssl_opts: [],
    tls: false,
    inbox_prefix: "_INBOX.",
    no_responders: false
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
  # you can customize default "_INBOX." inbox prefix with:
  {:ok, gnat} = Gnat.start_link(%{host: '127.0.0.1', port: 4222, inbox_prefix: "my_prefix._INBOX."})
  # you can use IPv6 addresses too
  {:ok, gnat} = Gnat.start_link(%{host: '::1', port: 4222, tcp_opts: [:inet6, :binary]})
  ```

  You can also pass arbitrary SSL or TCP options in the `tcp_opts` and `ssl_opts` keys.
  If you pass custom TCP options please include `:binary`. Gnat uses binary matching to parse messages.

  The final `opts` argument will be passed to the `GenServer.start_link` call so you can pass things like `[name: :gnat_connection]`.
  """
  @spec start_link(connection_settings(), keyword()) :: GenServer.on_start
  def start_link(connection_settings \\ %{}, opts \\ []) do
    GenServer.start_link(__MODULE__, connection_settings, opts)
  end

  @doc """
  Gracefully shuts down a connection

  ```
  {:ok, gnat} = Gnat.start_link()
  :ok = Gnat.stop(gnat)
  ```
  """
  @spec stop(t()) :: :ok
  def stop(pid), do: GenServer.call(pid, :stop)

  @doc """
  Subscribe to a topic

  Supported options:
    * queue_group: a string that identifies which queue group you want to join

  By default each subscriber will receive a copy of every message on the topic.
  When a queue_group is supplied messages will be spread among the subscribers
  in the same group. (see [nats queueing](https://nats.io/documentation/concepts/nats-queueing/))

  The subscribed process will begin receiving messages with a structure of `t:sent_message/0`

  ```
  {:ok, gnat} = Gnat.start_link()
  {:ok, subscription} = Gnat.sub(gnat, self(), "topic")
  receive do
    {:msg, %{topic: "topic", body: body}} ->
      IO.puts "Received: \#\{body\}"
  end
  ```
  """
  @spec sub(t(), pid(), String.t, keyword()) :: {:ok, non_neg_integer()} | {:ok, String.t} | {:error, String.t}
  def sub(pid, subscriber, topic, opts \\ []) do
    start = :erlang.monotonic_time()
    result = GenServer.call(pid, {:sub, subscriber, topic, opts})
    latency = :erlang.monotonic_time() - start
    :telemetry.execute([:gnat, :sub], %{latency: latency}, %{topic: topic})
    result
  end

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

  If you want to publish a message with headers you can pass the `:headers` key in the `opts` like this.

  ```
  {:ok, gnat} = Gnat.start_link()
  :ok = Gnat.pub(gnat, "listen", "Yo", headers: [{"foo", "bar"}])
  ```

  Headers must be passed as a `t:headers()` value (a list of tuples).
  Sending and parsing headers has more overhead than typical nats messages
  (see [the Nats 2.2 release notes for details](https://docs.nats.io/whats_new_22#message-headers)),
  so only use them when they are really valuable.
  """
  @spec pub(t(), String.t, binary(), keyword()) :: :ok
  def pub(pid, topic, message, opts \\ []) do
    start = :erlang.monotonic_time()
    opts = prepare_headers(opts)
    result = GenServer.call(pid, {:pub, topic, message, opts})
    latency = :erlang.monotonic_time() - start
    :telemetry.execute([:gnat, :pub], %{latency: latency} , %{topic: topic})
    result
  end

  @doc """
  Send a request and listen for a response synchronously

  Following the nats [request-response pattern](http://nats.io/documentation/concepts/nats-req-rep/) this
  function generates a one-time topic to receive replies and then sends a message to the provided topic.

  Supported options:
    * receive_timeout: an integer number of milliseconds to wait for a response. Defaults to 60_000
    * headers: a set of headers you want to send with the request (see `Gnat.pub/4`)

  ```
  {:ok, gnat} = Gnat.start_link()
  case Gnat.request(gnat, "i_can_haz_cheezburger", "plZZZZ?!?!?") do
    {:ok, %{body: delicious_cheezburger}} -> :yum
    {:error, :timeout} -> :sad_cat
  end
  ```

  ## No Responders

  If you send a request to a topic that has no registered listeners, it is sometimes convenient to find out
  right away, rather than waiting for a timeout to occur. In order to support this use-case, you can start
  your Gnat connection with the `no_responders: true` option and this function will return very quickly with
  an `{:error, :no_responders}` value. This behavior also works with `request_multi/4`
  """
  @spec request(t(), String.t, binary(), keyword()) :: {:ok, message} | {:error, :timeout} | {:error, :no_responders}
  def request(pid, topic, body, opts \\ []) do
    start = :erlang.monotonic_time()
    receive_timeout = Keyword.get(opts, :receive_timeout, 60_000)
    req = %{recipient: self(), body: body, topic: topic}
    opts = prepare_headers(opts)
    req =
      case Keyword.get(opts, :headers) do
        nil -> req
        headers -> Map.put(req, :headers, headers)
      end

    {:ok, subscription} = GenServer.call(pid, {:request, req})
    response = receive_request_response(subscription, receive_timeout)
    :ok = unsub(pid, subscription)
    latency = :erlang.monotonic_time() - start
    :telemetry.execute([:gnat, :request],  %{latency: latency}, %{topic: topic})
    response
  end

  @doc """
  Send a request and listen for multiple responses synchronously

  This function makes it easy to do a scatter-gather operation where you wait for a limited time
  and optionally a maximum number of replies.

  Supported options:
    * receive_timeout: an integer number of milliseconds to wait for responses. Defaults to 60_000
    * max_messages: an integer number of messages to listen for. Defaults to -1 meaning unlimited
    * headers: a set of headers you want to send with the request (see `Gnat.pub/4`)

  ```
  {:ok, gnat} = Gnat.start_link()
  {:ok, responses} = Gnat.request_multi(gnat, "i_can_haz_fries", "plZZZZZ!?!?", max_messages: 5)
  Enum.count(responses) #=> 5
  ```
  """
  @spec request_multi(t(), String.t(), binary(), keyword()) :: {:ok, list(message())} | {:error, :no_responders}
  def request_multi(pid, topic, body, opts \\ []) do
    start = :erlang.monotonic_time()
    receive_timeout_ms = Keyword.get(opts, :receive_timeout, 60_000)
    expiration = System.monotonic_time(:millisecond) + receive_timeout_ms
    max_messages = Keyword.get(opts, :max_messages, -1)

    req = %{recipient: self(), body: body, topic: topic}
    opts = prepare_headers(opts)
    req =
      case Keyword.get(opts, :headers) do
        nil -> req
        headers -> Map.put(req, :headers, headers)
      end

    {:ok, subscription} = GenServer.call(pid, {:request, req})
    result =
      case receive_multi_request_responses(subscription, expiration, max_messages) do
        {:error, :no_responders} -> {:error, :no_responders}
        responses when is_list(responses) -> {:ok, responses}
      end
    :ok = unsub(pid, subscription)
    latency = :erlang.monotonic_time() - start
    :telemetry.execute([:gnat, :request_multi],  %{latency: latency}, %{topic: topic})
    result
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
  @spec unsub(t(), non_neg_integer() | String.t, keyword()) :: :ok
  def unsub(pid, sid, opts \\ []) do
    start = :erlang.monotonic_time()
    result = GenServer.call(pid, {:unsub, sid, opts})
    :telemetry.execute([:gnat, :unsub], %{latency: :erlang.monotonic_time() - start})
    result
  end

  @doc """
  Ping the NATS server

  This correlates to the [PING](http://nats.io/documentation/internals/nats-protocol/#PINGPONG) command in the NATS protocol.
  If the NATS server responds with a PONG message this function will return `:ok`
  ```
  {:ok, gnat} = Gnat.start_link()
  :ok = Gnat.ping(gnat)
  ```
  """
  @deprecated "Pinging is handled internally by the connection, this functionality will be removed"
  def ping(pid) do
    GenServer.call(pid, {:ping, self()})
    receive do
      :pong -> :ok
    after
      3_000 -> {:error, "No PONG response after 3 sec"}
    end
  end

  @doc "get the number of active subscriptions"
  @spec active_subscriptions(t()) :: {:ok, non_neg_integer()}
  def active_subscriptions(pid) do
    GenServer.call(pid, :active_subscriptions)
  end

  @doc """
  Get information about the NATS server the connection is for
  """
  @spec server_info(t()) :: server_info()
  def server_info(name) do
    GenServer.call(name, :server_info)
  end

  @impl GenServer
  def init(connection_settings) do
    connection_settings = Map.merge(@default_connection_settings, connection_settings)
    case Gnat.Handshake.connect(connection_settings) do
      {:ok, socket, server_info} ->
        parser = Parsec.new

        request_inbox_prefix = Map.fetch!(connection_settings, :inbox_prefix) <> "#{nuid()}."

        state = %{socket: socket,
                  connection_settings: connection_settings,
                  server_info: server_info,
                  next_sid: 1,
                  receivers: %{},
                  parser: parser,
                  request_receivers: %{},
                  request_inbox_prefix: request_inbox_prefix}

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
      {new_parser, messages} = Parsec.parse(parser, data)
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
    pub =
      case request do
        %{headers: headers} ->
          Command.build(:pub, request.topic, request.body, headers: headers, reply_to: inbox)
        _ ->
          Command.build(:pub, request.topic, request.body, reply_to: inbox)
      end
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
  def handle_call(:server_info, _from, state) do
    {:reply, state.server_info, state}
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

  defp prepare_headers(opts) do
    if Keyword.has_key?(opts, :headers) do
      headers = :cow_http.headers(Keyword.get(opts, :headers))
      Keyword.put(opts, :headers, headers)
    else
      opts
    end
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
      :telemetry.execute([:gnat, :message_received], %{count: 1}, %{topic: topic})
      send state.receivers[sid].recipient, {:msg, %{topic: topic, body: body, reply_to: reply_to, sid: sid, gnat: self()}}
      update_subscriptions_after_delivering_message(state, sid)
    else
      Logger.error "#{__MODULE__} got message for sid #{sid}, but that is no longer registered"
      state
    end
  end
  defp process_message({:hmsg, topic, @request_sid, reply_to, status, description, headers, body}, state) do
    if Map.has_key?(state.request_receivers, topic) do
      map = %{
        topic: topic,
        body: body,
        reply_to: reply_to,
        gnat: self(),
        headers: headers,
        status: status,
        description: description
      }
      send state.request_receivers[topic], {:msg, map}
      state
    else
      Logger.error "#{__MODULE__} got a response for a request, but that is no longer registered"
      state
    end
  end
  defp process_message({:hmsg, topic, sid, reply_to, status, description, headers, body}, state) do
    unless is_nil(state.receivers[sid]) do
      :telemetry.execute([:gnat, :message_received], %{count: 1}, %{topic: topic})
      map = %{
        topic: topic,
        body: body,
        reply_to: reply_to,
        sid: sid,
        gnat: self(),
        headers: headers,
        status: status,
        description: description
      }
      send state.receivers[sid].recipient, {:msg, map}
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

  defp receive_multi_request_responses(_sub, _exp, 0), do: []

  defp receive_multi_request_responses(subscription, expiration, max_messages) do
    timeout = expiration - :erlang.monotonic_time(:millisecond)
    cond do
      timeout < 1 ->
        []
      true ->
        case receive_request_response(subscription, timeout) do
          {:error, :no_responders} ->
            {:error, :no_responders}
          {:error, :timeout} ->
            []
          {:ok, msg} ->
            [msg | receive_multi_request_responses(subscription, expiration, max_messages - 1)]
        end
    end
  end

  defp receive_request_response(subscription, timeout) do
    receive do
      {:msg, %{topic: ^subscription, status: "503"}} ->
        {:error, :no_responders}
      {:msg, %{topic: ^subscription}=msg} ->
        {:ok, msg}
      after timeout ->
        {:error, :timeout}
    end
  end
end
