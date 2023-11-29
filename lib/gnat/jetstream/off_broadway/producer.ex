with {:module, _} <- Code.ensure_compiled(Broadway) do
  defmodule OffBroadway.Jetstream.Producer do
    @moduledoc """
    A GenStage producer meant to work with [Broadway](https://github.com/dashbitco/broadway).
    It continuously receives messages from a NATS JetStream and acknowledges them after being
    successfully processed.

    ## Options

    ### Connection options

    The following options are mandatory.

    * `connection_name` - The name of Gnat process or Gnat connection supervisor.

    * `stream_name` - The name of stream to consume from.

    * `consumer_name` - The name of consumer.


    Optional options:

    * `connection_retry_timeout` - time in milliseconds, after which the failing
      connection will retry. Defaults to `1000`.

    * `connection_retries` - number of failing connection retries the producer will
      make before shutting down. Defaults to `10`.

    * `inbox_prefix` - custom prefix for listening topic. Defaults to `_INBOX.`.

    * `domain` - Jetstream domain.

    ### Message pulling options

    * `receive_interval` - The duration in milliseconds for which the producer waits
      before making a request for more messages. Defaults to `5000`.

    * `receive_timeout` - The maximum time to wait for NATS Jetstream to respond with
      a requested message. Defaults to `:infinity`.

    ### Acknowledger options

    These options are passed to the acknowledger on its initialization.

    * `:on_success` - Configures the behaviour for successful messages. Defaults to `:ack`.

    * `:on_failure` - Configures the behaviour for failed messages. Defaults to `:nack`.

    ## Acknowledgements

    By default, successful messages are acknowledged, and failed ones are nack'ed. You can
    change this behaviour by setting the `:on_success` or/and `:on_failure` options.

    You can also change the acknowledgement action for individual messages using the
    `Broadway.Message.configure_ack/2` function.

    The supported options are:

    * `:ack` - Acknowledges a message was completely handled.

    * `:nack` - Signals that the message will not be processed now and will be redelivered.

    * `:term` - Tells the server to stop redelivery of a message without acknowledging it.

    ## Example

    Example Broadway module definition:

    ```
    defmodule MyBroadway do
      use Broadway

      def start_link(_opts) do
        Broadway.start_link(
          __MODULE__,
          name: MyBroadway,
          producer: [
            module: {
              OffBroadway.Jetstream.Producer,
              connection_name: :gnat,
              stream_name: "TEST_STREAM",
              consumer_name: "TEST_CONSUMER"
            },
            concurrency: 10
          ],
          processors: [
            default: [concurrency: 10]
          ],
          batchers: [
            example: [
              concurrency: 5,
              batch_size: 10,
              batch_timeout: 2_000
            ]
          ]
        )
      end

      def handle_message(_processor_name, message, _context) do
        message
        |> Message.update_data(&process_data/1)
        |> Message.put_batcher(:example)
      end

      defp process_data(data) do
        # Some data processing
      end

      def handle_batch(:example, messages, _batch_info, _context) do
        # Do something with batch messages
      end

    end
    ```

    Learn more about available options in [Broadway documentation](https://hexdocs.pm/broadway/Broadway.html).

    Once you have your Broadway pipeline defined, you can add it to your supervision tree:

    ```
    children = [
      {MyBroadway, []}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
    ```
    """

    require Logger

    use GenStage

    alias Broadway.Message
    alias Broadway.Producer
    alias Gnat.Jetstream.PullConsumer.ConnectionOptions
    alias Gnat.Jetstream.API.Util
    alias OffBroadway.Jetstream.Acknowledger

    @behaviour Producer

    @default_receive_interval 5_000
    @default_receive_timeout :infinity

    @impl Producer
    def prepare_for_start(_module, broadway_opts) do
      {producer_module, module_opts} = broadway_opts[:producer][:module]

      broadway_opts_with_defaults =
        put_in(broadway_opts, [:producer, :module], {producer_module, module_opts})

      {[], broadway_opts_with_defaults}
    end

    @impl true
    def init(opts) do
      receive_interval = opts[:receive_interval] || @default_receive_interval
      receive_timeout = opts[:receive_timeout] || @default_receive_timeout

      connection_options =
        opts
        |> Keyword.take([
          :connection_name,
          :stream_name,
          :consumer_name,
          :connection_retry_timeout,
          :connection_retries,
          :inbox_prefix,
          :domain
        ])
        |> ConnectionOptions.validate!()

      listening_topic = Util.reply_inbox(connection_options.inbox_prefix)

      case Acknowledger.init(opts) do
        {:ok, ack_ref} ->
          send(self(), :connect)

          {:producer,
           %{
             demand: 0,
             receive_timer: nil,
             receive_interval: receive_interval,
             receive_timeout: receive_timeout,
             connection_options: connection_options,
             connection_pid: nil,
             connection_retries_left: nil,
             subscription_id: nil,
             status: :disconnected,
             listening_topic: listening_topic,
             ack_ref: ack_ref
           }}

        {:error, message} ->
          raise ArgumentError, message
      end
    end

    @impl true
    def handle_demand(incoming_demand, %{demand: demand} = state) do
      handle_receive_messages(%{state | demand: demand + incoming_demand})
    end

    @impl true
    def handle_info(
          :connect,
          %{
            connection_options:
              %ConnectionOptions{
                connection_name: connection_name,
                connection_retries: retries,
                connection_retry_timeout: retry_timeout
              } = conn_options,
            listening_topic: listening_topic,
            connection_retries_left: retries_left
          } = state
        ) do
      Logger.debug(
        """
        #{__MODULE__} for #{conn_options.stream_name}.#{conn_options.consumer_name} \
        is connecting to Gnat.
        """,
        listening_topic: listening_topic,
        connection_name: connection_name
      )

      case Process.whereis(connection_name) do
        nil when retries_left > 0 ->
          Logger.debug(
            """
            #{__MODULE__} for #{conn_options.stream_name}.#{conn_options.consumer_name} \
            failed to connect to Gnat and will retry.
            """,
            listening_topic: listening_topic,
            connection_name: connection_name
          )

          retries_left = if retries_left, do: retries_left - 1, else: retries

          Process.send_after(self(), :connect, retry_timeout)
          {:noreply, [], %{state | connection_retries_left: retries_left}}

        nil ->
          Logger.error(
            """
            #{__MODULE__} for #{conn_options.stream_name}.#{conn_options.consumer_name} \
            failed to connect to NATS and retries limit has been exhausted. Stopping.
            """,
            listening_topic: listening_topic,
            connection_name: connection_name
          )

          {:stop, {:shutdown, :connection_failed}, state}

        connection_pid ->
          Process.monitor(connection_pid)

          {:ok, sid} = Gnat.sub(connection_name, self(), listening_topic)

          {:noreply, [],
           %{
             state
             | status: :connected,
               connection_pid: connection_pid,
               connection_retries_left: nil,
               subscription_id: sid
           }}
      end
    end

    def handle_info(:receive_messages, %{status: :disconnected} = state) do
      {:noreply, [], state}
    end

    def handle_info(:receive_messages, %{receive_timer: nil} = state) do
      {:noreply, [], state}
    end

    def handle_info(:receive_messages, state) do
      handle_receive_messages(%{state | receive_timer: nil})
    end

    def handle_info(
          {:DOWN, _ref, :process, connection_pid, _reason},
          %{
            connection_pid: connection_pid,
            connection_options:
              %ConnectionOptions{connection_retry_timeout: retry_timeout} = conn_options
          } = state
        ) do
      Logger.debug(
        """
        #{__MODULE__} for #{conn_options.stream_name}.#{conn_options.consumer_name} \
        NATS connection has died. Producer is reconnecting.
        """,
        listening_topic: state.listening_topic,
        subscription_id: state.subscription_id,
        connection_name: state.connection_options.connection_name
      )

      Process.send_after(self(), :connect, retry_timeout)
      {:noreply, [], %{state | status: :disconnected, connection_pid: nil}}
    end

    def handle_info(unexpected_message, %{connection_options: conn_options} = state) do
      Logger.debug(
        """
        #{__MODULE__} for #{conn_options.stream_name}.#{conn_options.consumer_name} \
        received unexpected message: #{inspect(unexpected_message, pretty: true)}
        """,
        listening_topic: state.listening_topic,
        subscription_id: state.subscription_id,
        connection_name: state.connection_options.connection_name
      )

      {:noreply, [], state}
    end

    @impl true
    def prepare_for_draining(%{receive_timer: receive_timer} = state) do
      receive_timer && Process.cancel_timer(receive_timer)
      {:noreply, [], %{state | receive_timer: nil}}
    end

    defp handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
      messages = receive_messages_from_jetstream(state, demand)
      new_demand = demand - length(messages)

      receive_timer =
        case {messages, new_demand} do
          {[], _} -> schedule_receive_messages(state.receive_interval)
          {_, 0} -> nil
          _ -> schedule_receive_messages(0)
        end

      {:noreply, messages, %{state | demand: new_demand, receive_timer: receive_timer}}
    end

    defp handle_receive_messages(state) do
      {:noreply, [], state}
    end

    defp receive_messages_from_jetstream(state, total_demand) do
      request_messages_from_jetstream(total_demand, state)

      do_receive_messages(total_demand, state.listening_topic, state.receive_timeout)
      |> wrap_received_messages(state.ack_ref)
    end

    defp request_messages_from_jetstream(total_demand, state) do
      Gnat.Jetstream.API.Consumer.request_next_message(
        state.connection_options.connection_name,
        state.connection_options.stream_name,
        state.connection_options.consumer_name,
        state.listening_topic,
        state.connection_options.domain,
        batch: total_demand,
        no_wait: true
      )
    end

    defp do_receive_messages(total_demand, listening_topic, receive_timeout) do
      Enum.reduce_while(1..total_demand, [], fn _, acc ->
        receive do
          {:msg, %{reply_to: "$JS.ACK" <> _} = msg} ->
            {:cont, [msg | acc]}

          {:msg, %{topic: ^listening_topic}} ->
            {:halt, acc}
        after
          receive_timeout ->
            {:halt, acc}
        end
      end)
    end

    defp wrap_received_messages(jetstream_messages, ack_ref) do
      Enum.map(jetstream_messages, &jetstream_msg_to_broadway_msg(&1, ack_ref))
    end

    defp jetstream_msg_to_broadway_msg(jetstream_message, ack_ref) do
      acknowledger = Acknowledger.builder(ack_ref).(jetstream_message.reply_to)

      %Message{
        data: jetstream_message.body,
        metadata: %{
          topic: jetstream_message.topic,
          headers: Map.get(jetstream_message, :headers, [])
        },
        acknowledger: acknowledger
      }
    end

    defp schedule_receive_messages(interval) do
      Process.send_after(self(), :receive_messages, interval)
    end
  end
end
