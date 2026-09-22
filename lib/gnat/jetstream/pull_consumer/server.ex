defmodule Gnat.Jetstream.PullConsumer.Server do
  @moduledoc false

  require Logger

  use Connection

  alias Gnat.Jetstream.PullConsumer.ConnectionOptions
  alias Gnat.Jetstream.API.Util

  defstruct [
    :connection_options,
    :state,
    :listening_topic,
    :module,
    :subscription_id,
    :connection_pid,
    :connection_monitor_ref,
    :subscription_request,
    :consumer_name,
    :ack_policy,
    :last_response_at,
    current_retry: 0,
    buffer: []
  ]

  def init(%{module: module, init_arg: init_arg}) do
    _ = Process.put(:"$initial_call", {module, :init, 1})

    case module.init(init_arg) do
      {:ok, state, connection_options} when is_list(connection_options) ->
        Process.flag(:trap_exit, true)

        connection_options = ConnectionOptions.validate!(connection_options)

        gen_state = %__MODULE__{
          connection_options: connection_options,
          state: state,
          listening_topic: Util.reply_inbox(connection_options.inbox_prefix),
          module: module,
          consumer_name: connection_options.consumer_name
        }

        schedule_heartbeat_check(gen_state)

        {:connect, :init, gen_state}

      :ignore ->
        :ignore

      {:stop, _} = stop ->
        stop
    end
  end

  def connect({:subscription_error, reason}, gen_state) do
    connection_failed(reason, gen_state)
  end

  def connect(
        _,
        %__MODULE__{
          connection_options: %ConnectionOptions{
            stream_name: stream_name,
            consumer: consumer,
            connection_name: connection_name,
            domain: domain
          },
          listening_topic: listening_topic,
          consumer_name: consumer_name,
          module: module
        } = gen_state
      ) do
    Logger.debug(
      "#{__MODULE__} for #{stream_name}.#{gen_state.consumer_name} is connecting to Gnat.",
      module: module,
      listening_topic: listening_topic,
      connection_name: connection_name
    )

    # Each pull subscription has its own inbox so terminal statuses from an
    # abandoned request can't change the replacement request's accounting.
    listening_topic =
      Util.reply_inbox(gen_state.connection_options.inbox_prefix)

    with {:ok, conn} <- connection_pid(connection_name),
         {:ok, consumer_info} <-
           ensure_consumer_exists(
             conn,
             stream_name,
             consumer_name,
             consumer,
             domain
           ),
         :ok <- validate_batch_ack_policy(gen_state.connection_options, consumer_info) do
      gen_state = %{
        gen_state
        | consumer_name: consumer_info.name,
          ack_policy: consumer_info.config.ack_policy
      }

      subscribe(conn, listening_topic, consumer_info, gen_state)
    else
      {:error, reason} -> connection_failed(reason, gen_state)
    end
  end

  defp subscribe(conn, listening_topic, consumer_info, gen_state) do
    request = Gnat.sub_async(conn, self(), listening_topic)

    {:ok,
     %{
       gen_state
       | connection_pid: conn,
         listening_topic: listening_topic,
         subscription_request: {:subscribe, request, consumer_info}
     }}
  end

  defp subscription_ready(sid, consumer_info, gen_state) do
    conn = gen_state.connection_pid
    gen_state = %{gen_state | subscription_id: sid, subscription_request: nil}

    monitor_ref = Process.monitor(conn)
    state = maybe_handle_connected(gen_state.module, consumer_info, gen_state.state)
    gen_state = %{gen_state | connection_monitor_ref: monitor_ref, state: state}

    case initial_fetch(gen_state, conn) do
      :ok ->
        {:noreply, touch_response(%{gen_state | current_retry: 0})}

      {:error, reason} ->
        {:noreply, reset_to_disconnected(gen_state, false, {:subscription_error, reason})}
    end
  end

  defp connection_failed(reason, gen_state) do
    %{
      connection_options: %ConnectionOptions{
        stream_name: stream_name,
        connection_name: connection_name,
        connection_retry_timeout: connection_retry_timeout,
        connection_retries: connection_retries
      }
    } = gen_state

    if gen_state.current_retry >= connection_retries do
      Logger.error(
        "#{__MODULE__} for #{stream_name}.#{gen_state.consumer_name} exhausted connection retries.",
        module: gen_state.module,
        connection_name: connection_name
      )

      {:stop, :timeout, %{gen_state | current_retry: 0}}
    else
      Logger.debug(
        "#{__MODULE__} for #{stream_name}.#{gen_state.consumer_name} will retry connecting: #{inspect(reason)}",
        module: gen_state.module,
        connection_name: connection_name
      )

      {:backoff, connection_retry_timeout,
       %{gen_state | current_retry: gen_state.current_retry + 1}}
    end
  end

  defp ensure_consumer_exists(gnat, stream_name, consumer_name, consumer, domain)
       when is_binary(consumer_name) do
    try do
      case Gnat.Jetstream.API.Consumer.info(gnat, stream_name, consumer_name, domain) do
        {:ok, consumer_info} ->
          {:ok, consumer_info}

        {:error, %{"err_code" => 10014}} when not is_nil(consumer) ->
          ensure_consumer_exists(gnat, stream_name, nil, consumer, domain)

        {:error, reason} ->
          {:error, reason}
      end
    catch
      :exit, reason -> {:error, {:process_exit, reason}}
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp ensure_consumer_exists(gnat, _stream_name, nil, consumer_struct, _domain) do
    # Ephemeral or auto-cleanup durable consumer case - create it
    try do
      with {:ok, consumer_definition} <- validate_consumer_for_creation(consumer_struct),
           {:ok, consumer_info} <- Gnat.Jetstream.API.Consumer.create(gnat, consumer_definition) do
        {:ok, consumer_info}
      end
    catch
      :exit, reason -> {:error, {:process_exit, reason}}
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp validate_consumer_for_creation(consumer_definition) do
    cond do
      consumer_definition.durable_name == nil && consumer_definition.inactive_threshold != nil ->
        {:error, "ephemeral consumers (durable_name: nil) cannot have inactive_threshold set"}

      consumer_definition.durable_name != nil && consumer_definition.inactive_threshold == nil ->
        {:error,
         "durable consumers specified via :consumer must have inactive_threshold set for auto-cleanup"}

      true ->
        {:ok, consumer_definition}
    end
  end

  defp validate_batch_ack_policy(%ConnectionOptions{batch_size: batch_size}, consumer_info)
       when batch_size > 1 do
    case consumer_info.config.ack_policy do
      policy when policy in [:explicit, :all] ->
        :ok

      other ->
        {:error,
         "batch_size > 1 requires ack_policy: :explicit or :all on the consumer, " <>
           "got: #{inspect(other)}"}
    end
  end

  defp validate_batch_ack_policy(_connection_options, _consumer_info), do: :ok

  defp maybe_handle_connected(module, consumer_info, state) do
    if function_exported?(module, :handle_connected, 2) do
      {:ok, state} = module.handle_connected(consumer_info, state)
      state
    else
      state
    end
  end

  defp maybe_handle_status(message, %__MODULE__{module: module, state: state} = gen_state) do
    if function_exported?(module, :handle_status, 2) do
      {:ok, new_state} = module.handle_status(message, state)
      touch_response(%{gen_state | state: new_state})
    else
      gen_state
    end
  end

  defp connection_pid(connection_name) when is_pid(connection_name) do
    if Process.alive?(connection_name) do
      {:ok, connection_name}
    else
      {:error, :not_alive}
    end
  end

  defp connection_pid(connection_name) do
    case Process.whereis(connection_name) do
      nil -> {:error, :not_found}
      pid -> {:ok, pid}
    end
  end

  # Subscription identity includes the connection PID because subscription IDs
  # are reused by replacement Gnat processes. Recovery drains accepted deliveries
  # before replacing this identity; unrelated messages must not affect a new pull.
  def handle_info(message, %{subscription_request: {operation, request, context}} = gen_state) do
    case Gnat.subscription_response(message, request) do
      {:reply, {:ok, sid}} when operation == :subscribe ->
        subscription_ready(sid, context, gen_state)

      {:reply, :ok} when operation == :unsubscribe ->
        subscription_retired(gen_state, context)

      {:reply, {:error, reason}} when operation == :subscribe ->
        subscription_failed(reason, gen_state)

      {:error, {reason, _conn}} ->
        case operation do
          :subscribe -> subscription_failed(reason, gen_state)
          :unsubscribe -> subscription_retired(gen_state, %{context | acknowledge?: false})
        end

      :no_reply ->
        handle_subscription_message(message, gen_state)
    end
  end

  def handle_info(
        {:msg, %{gnat: msg_gnat, sid: msg_sid} = message},
        %__MODULE__{connection_pid: conn, subscription_id: sid} = gen_state
      )
      when msg_gnat != conn or msg_sid != sid do
    Logger.warning(
      "#{__MODULE__} dropping message from stale subscription " <>
        "(msg=#{inspect(msg_gnat)}/#{inspect(msg_sid)}, " <>
        "current=#{inspect(conn)}/#{inspect(sid)}, " <>
        "topic=#{inspect(Map.get(message, :topic))}, " <>
        "status=#{inspect(Map.get(message, :status))})",
      module: gen_state.module,
      listening_topic: gen_state.listening_topic,
      connection_name: gen_state.connection_options.connection_name
    )

    {:noreply, gen_state}
  end

  def handle_info(
        {:msg, %{status: status, description: description} = message},
        %__MODULE__{} = gen_state
      )
      when (status == "409" and description == "Consumer Deleted") or
             (status == "404" and description == "Consumer Not Found") do
    gen_state = maybe_handle_status(message, gen_state)
    {:noreply, reset_to_disconnected(gen_state)}
  end

  # -- 100 is an idle heartbeat — the pull is still alive, do nothing but
  # feed the watchdog and invoke the user callback. Applies to both
  # single-message and batch modes; do NOT re-pull (issuing a fresh pull on
  # every heartbeat causes the server-side pull queue to grow without
  # bound). --
  def handle_info({:msg, %{status: "100"} = message}, %__MODULE__{} = gen_state) do
    gen_state = touch_response(gen_state)
    gen_state = maybe_handle_status(message, gen_state)
    {:noreply, gen_state}
  end

  # -- Batch mode: any other status (404/408 terminators, 409 leadership
  # change / max_ack_pending / max_waiting / consumer-deleted, etc.) ends
  # the outstanding pull request. Process any partial buffer and issue a
  # new pull so the consumer doesn't stall. --
  def handle_info(
        {:msg, %{status: status, gnat: gnat} = message},
        %__MODULE__{
          connection_options: %ConnectionOptions{batch_size: batch_size},
          buffer: buffer
        } = gen_state
      )
      when batch_size > 1 and is_binary(status) and status != "" do
    gen_state = touch_response(gen_state)
    gen_state = maybe_handle_status(message, gen_state)

    case buffer do
      [] ->
        # Nothing buffered — long-poll for new messages.
        continue_after_send(gen_state, request_batch(gnat, gen_state, :tailing))

      _messages ->
        # Partial batch — process what we have, then try for more.
        process_batch_and_fetch(gen_state, gnat)
    end
  end

  # -- Single-message mode: informational status. Drop + re-pull so the
  # consumer doesn't stall. Matches the nats.go convention of never exposing
  # status messages to the user's message handler. --
  def handle_info(
        {:msg, %{status: status} = message},
        %__MODULE__{} = gen_state
      )
      when is_binary(status) and status != "" do
    gen_state = touch_response(gen_state)
    gen_state = maybe_handle_status(message, gen_state)

    continue_after_send(gen_state, next_message(message.gnat, gen_state))
  end

  # -- Batch mode: data message — buffer until batch is full --
  def handle_info(
        {:msg, message},
        %__MODULE__{
          connection_options: %ConnectionOptions{batch_size: batch_size},
          buffer: buffer
        } = gen_state
      )
      when batch_size > 1 do
    gen_state = touch_response(gen_state)
    buffer = [message | buffer]
    gen_state = %{gen_state | buffer: buffer}

    if length(buffer) >= batch_size do
      process_batch_and_fetch(gen_state, message.gnat)
    else
      {:noreply, gen_state}
    end
  end

  # -- Single-message mode (batch_size == 1, the default) --
  def handle_info(
        {:msg, message},
        %__MODULE__{
          connection_options: %ConnectionOptions{
            stream_name: stream_name,
            connection_name: connection_name,
            request_expires: request_expires,
            idle_heartbeat: idle_heartbeat
          },
          listening_topic: listening_topic,
          subscription_id: subscription_id,
          state: state,
          module: module,
          consumer_name: consumer_name
        } = gen_state
      ) do
    gen_state = touch_response(gen_state)

    Logger.debug(
      """
      #{__MODULE__} for #{stream_name}.#{consumer_name} received a message: \
      #{inspect(message, pretty: true)}
      """,
      module: module,
      listening_topic: listening_topic,
      subscription_id: subscription_id,
      connection_name: connection_name
    )

    {action, state} = module.handle_message(message, state)
    gen_state = touch_response(%{gen_state | state: state})

    result =
      case action do
        :ack ->
          connection_result(fn ->
            Gnat.Jetstream.ack_next(message, listening_topic,
              batch: 1,
              expires: request_expires,
              idle_heartbeat: idle_heartbeat
            )
          end)

        action when action in [:nack, :term, :noreply] ->
          with :ok <- acknowledge(message, action) do
            next_message(message.gnat, gen_state)
          end
      end

    continue_after_send(gen_state, result)
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %__MODULE__{
          connection_options: %ConnectionOptions{
            connection_name: connection_name,
            stream_name: stream_name
          },
          subscription_id: subscription_id,
          listening_topic: listening_topic,
          module: module,
          connection_monitor_ref: monitor_ref,
          consumer_name: consumer_name
        } = gen_state
      )
      when ref == monitor_ref do
    Logger.debug(
      """
      #{__MODULE__} for #{stream_name}.#{consumer_name}:
      NATS connection has died. PullConsumer is reconnecting.
      """,
      module: module,
      listening_topic: listening_topic,
      subscription_id: subscription_id,
      connection_name: connection_name
    )

    {:noreply, reset_to_disconnected(gen_state, false)}
  end

  # -- Heartbeat watchdog: periodic check for "have we heard anything from
  # the server recently?". Runs on a fixed cadence regardless of connection
  # state. While disconnected (last_response_at == nil) it does nothing
  # except reschedule itself. While connected, if the gap since the last
  # inbound message exceeds `2 * idle_heartbeat`, we treat the pull as
  # stuck and force a reconnect — this catches dropped pull requests where
  # the TCP connection is otherwise healthy and no 408/409 is forthcoming. --
  def handle_info(:heartbeat_check, %__MODULE__{} = gen_state) do
    schedule_heartbeat_check(gen_state)

    case heartbeat_status(gen_state) do
      :ok ->
        {:noreply, gen_state}

      {:expired, gap_ms, threshold_ms} ->
        %__MODULE__{
          connection_options: %ConnectionOptions{
            connection_name: connection_name,
            stream_name: stream_name
          },
          listening_topic: listening_topic,
          subscription_id: subscription_id,
          module: module,
          consumer_name: consumer_name
        } = gen_state

        Logger.warning(
          """
          #{__MODULE__} for #{stream_name}.#{consumer_name} has not received \
          any traffic from the server in #{gap_ms}ms (threshold #{threshold_ms}ms). \
          Forcing reconnect.
          """,
          module: module,
          listening_topic: listening_topic,
          subscription_id: subscription_id,
          connection_name: connection_name
        )

        :telemetry.execute(
          [:gnat, :jetstream, :pull_consumer, :heartbeat_expired],
          %{gap_ms: gap_ms, threshold_ms: threshold_ms},
          %{
            module: module,
            stream_name: stream_name,
            consumer_name: consumer_name,
            connection_name: connection_name
          }
        )

        # Tear down the old subscription and monitor before reconnecting so
        # we don't get a stale {:DOWN, ...} or stray inbox messages from
        # the connection we're abandoning.
        {:noreply, reset_to_disconnected(gen_state)}
    end
  end

  def handle_info(
        other,
        %__MODULE__{
          connection_options: %ConnectionOptions{
            connection_name: connection_name,
            stream_name: stream_name
          },
          subscription_id: subscription_id,
          listening_topic: listening_topic,
          module: module,
          consumer_name: consumer_name
        } = gen_state
      ) do
    Logger.debug(
      """
      #{__MODULE__} for #{stream_name}.#{consumer_name} received
      unexpected message: #{inspect(other, pretty: true)}
      """,
      module: module,
      listening_topic: listening_topic,
      subscription_id: subscription_id,
      connection_name: connection_name
    )

    {:noreply, gen_state}
  end

  def handle_call(:close, _from, gen_state) do
    # Gnat monitors subscribers and removes their subscriptions on exit, including
    # subscriptions created by requests still queued when the subscriber exits.
    {:stop, :shutdown, :ok, gen_state}
  end

  defp handle_subscription_message(:heartbeat_check, gen_state) do
    schedule_heartbeat_check(gen_state)
    {:noreply, gen_state}
  end

  defp handle_subscription_message(
         {:msg, %{gnat: conn, sid: sid} = message},
         %{connection_pid: conn, subscription_id: sid} = gen_state
       ) do
    case message do
      %{status: status} when is_binary(status) and status != "" ->
        {:noreply, gen_state}

      _ ->
        {:noreply, %{gen_state | buffer: [message | gen_state.buffer]}}
    end
  end

  defp handle_subscription_message(_message, gen_state), do: {:noreply, gen_state}

  defp subscription_failed(reason, gen_state) do
    gen_state = clear_subscription(gen_state)
    {:connect, {:subscription_error, reason}, gen_state}
  end

  defp subscription_retired(gen_state, %{acknowledge?: acknowledge?, reason: reason}) do
    gen_state =
      gen_state
      |> collect_delivered_messages()
      |> process_delivered_messages(acknowledge?)
      |> clear_subscription()

    {:connect, reason, gen_state}
  end

  defp next_message(conn, gen_state) do
    %{
      connection_options: %ConnectionOptions{
        stream_name: stream_name,
        domain: domain,
        request_expires: expires,
        idle_heartbeat: idle_heartbeat
      },
      consumer_name: consumer_name,
      listening_topic: listening_topic
    } = gen_state

    # Single-message-mode pulls long-poll the same way batch mode does in
    # :tailing — expires bounds the wait, idle_heartbeat keeps the watchdog
    # fed during quiet periods.
    connection_result(fn ->
      Gnat.Jetstream.API.Consumer.request_next_message(
        conn,
        stream_name,
        consumer_name,
        listening_topic,
        domain,
        expires: expires,
        idle_heartbeat: idle_heartbeat
      )
    end)
  end

  defp initial_fetch(gen_state, conn) do
    if gen_state.connection_options.batch_size > 1 do
      request_batch(conn, gen_state, :catching_up)
    else
      next_message(conn, gen_state)
    end
  end

  defp request_batch(conn, gen_state, mode) do
    %{
      connection_options: %ConnectionOptions{
        stream_name: stream_name,
        batch_size: batch_size,
        domain: domain,
        request_expires: expires,
        idle_heartbeat: idle_heartbeat
      },
      consumer_name: consumer_name,
      listening_topic: listening_topic
    } = gen_state

    opts =
      case mode do
        :catching_up ->
          # no_wait short-polls — server replies immediately with 404 if
          # the stream is empty, so heartbeats are unnecessary.
          [batch: batch_size, no_wait: true]

        :tailing ->
          [batch: batch_size, expires: expires, idle_heartbeat: idle_heartbeat]
      end

    connection_result(fn ->
      Gnat.Jetstream.API.Consumer.request_next_message(
        conn,
        stream_name,
        consumer_name,
        listening_topic,
        domain,
        opts
      )
    end)
  end

  # ---- Heartbeat watchdog helpers ----

  defp touch_response(%__MODULE__{} = gen_state) do
    %{gen_state | last_response_at: System.monotonic_time(:millisecond)}
  end

  defp schedule_heartbeat_check(%__MODULE__{
         connection_options: %ConnectionOptions{heartbeat_check_interval: interval}
       }) do
    Process.send_after(self(), :heartbeat_check, interval)
    :ok
  end

  defp heartbeat_status(%__MODULE__{last_response_at: nil}), do: :ok

  defp heartbeat_status(%__MODULE__{
         last_response_at: last,
         connection_options: %ConnectionOptions{idle_heartbeat: idle_heartbeat_ns}
       }) do
    threshold_ms = div(idle_heartbeat_ns, 1_000_000) * 2
    gap_ms = System.monotonic_time(:millisecond) - last

    if gap_ms > threshold_ms do
      {:expired, gap_ms, threshold_ms}
    else
      :ok
    end
  end

  # Keep the subscription identity until Gnat confirms retirement or exits.
  # Its request monitor also lets recovery wait without blocking this process.
  defp reset_to_disconnected(gen_state, acknowledge? \\ true, reason \\ :reconnect) do
    if gen_state.connection_monitor_ref do
      Process.demonitor(gen_state.connection_monitor_ref, [:flush])
    end

    request = Gnat.unsub_async(gen_state.connection_pid, gen_state.subscription_id)

    gen_state =
      if acknowledge? do
        gen_state
      else
        gen_state
        |> collect_delivered_messages()
        |> process_delivered_messages(false)
      end

    %{
      gen_state
      | connection_monitor_ref: nil,
        subscription_request:
          {:unsubscribe, request, %{acknowledge?: acknowledge?, reason: reason}},
        last_response_at: nil
    }
  end

  defp clear_subscription(gen_state) do
    %{
      gen_state
      | subscription_id: nil,
        subscription_request: nil,
        connection_pid: nil,
        connection_monitor_ref: nil,
        listening_topic: nil,
        buffer: [],
        last_response_at: nil
    }
  end

  defp process_delivered_messages(gen_state, acknowledge?) do
    acknowledge = if acknowledge?, do: &acknowledge/2, else: &skip_acknowledgement/2

    case process_and_ack_batch(gen_state, acknowledge) do
      {:ok, gen_state} ->
        gen_state

      {{:error, _reason}, gen_state} ->
        process_delivered_messages(gen_state, false)
    end
  end

  defp skip_acknowledgement(_message, action) when action in [:ack, :nack, :term, :noreply],
    do: :ok

  # Once Gnat replies to unsubscribe, that process can't
  # forward more messages for this subscription. Its earlier deliveries precede
  # the reply in our mailbox. The same ordering holds for a dead connection's
  # deliveries and its DOWN signal.
  defp collect_delivered_messages(%{connection_pid: conn, subscription_id: sid} = gen_state)
       when is_pid(conn) and is_integer(sid) do
    receive do
      {:msg, %{gnat: ^conn, sid: ^sid, status: status}} when is_binary(status) and status != "" ->
        collect_delivered_messages(gen_state)

      {:msg, %{gnat: ^conn, sid: ^sid} = message} ->
        collect_delivered_messages(%{gen_state | buffer: [message | gen_state.buffer]})
    after
      0 -> gen_state
    end
  end

  defp collect_delivered_messages(gen_state), do: gen_state

  defp process_and_ack_batch(gen_state, acknowledge \\ &acknowledge/2)

  defp process_and_ack_batch(%{buffer: []} = gen_state, _acknowledge), do: {:ok, gen_state}

  defp process_and_ack_batch(
         %{ack_policy: :all, connection_options: %{batch_size: batch_size}} = gen_state,
         acknowledge
       )
       when batch_size > 1 do
    %{buffer: [last | _] = buffer, module: module, state: state} = gen_state

    new_state =
      buffer
      |> Enum.reverse()
      |> Enum.reduce(state, fn message, acc_state ->
        case module.handle_message(message, acc_state) do
          {:ack, updated_state} ->
            updated_state

          other ->
            raise ArgumentError,
                  "batch mode with ack_policy: :all requires handle_message/2 to return {:ack, state}, " <>
                    "got: #{inspect(other)}. Use ack_policy: :explicit for per-message outcomes"
        end
      end)

    result = acknowledge.(last, :ack)
    {result, touch_response(%{gen_state | state: new_state, buffer: []})}
  end

  defp process_and_ack_batch(gen_state, acknowledge) do
    process_messages(Enum.reverse(gen_state.buffer), %{gen_state | buffer: []}, acknowledge)
  end

  defp process_messages([], gen_state, _acknowledge), do: {:ok, touch_response(gen_state)}

  defp process_messages([message | rest], gen_state, acknowledge) do
    {action, state} = gen_state.module.handle_message(message, gen_state.state)
    gen_state = %{gen_state | state: state}

    case acknowledge.(message, action) do
      :ok -> process_messages(rest, gen_state, acknowledge)
      error -> {error, %{gen_state | buffer: Enum.reverse(rest)}}
    end
  end

  defp process_batch_and_fetch(gen_state, conn) do
    case process_and_ack_batch(gen_state) do
      {:ok, gen_state} ->
        continue_after_send(gen_state, request_batch(conn, gen_state, :catching_up))

      {error, gen_state} ->
        continue_after_send(gen_state, error)
    end
  end

  defp continue_after_send(gen_state, :ok), do: {:noreply, gen_state}

  defp continue_after_send(gen_state, {:error, _reason}) do
    {:noreply, reset_to_disconnected(gen_state, false)}
  end

  defp connection_result(fun) do
    fun.()
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  defp acknowledge(message, action) do
    connection_result(fn ->
      case action do
        :ack -> Gnat.Jetstream.ack(message)
        :nack -> Gnat.Jetstream.nack(message)
        :term -> Gnat.Jetstream.ack_term(message)
        :noreply -> :ok
      end
    end)
  end
end
