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
    :consumer_name,
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

  def connect(
        _,
        %__MODULE__{
          connection_options: %ConnectionOptions{
            stream_name: stream_name,
            consumer_name: consumer_name,
            consumer: consumer,
            connection_name: connection_name,
            connection_retry_timeout: connection_retry_timeout,
            connection_retries: connection_retries,
            domain: domain
          },
          listening_topic: listening_topic,
          module: module
        } = gen_state
      ) do
    Logger.debug(
      "#{__MODULE__} for #{stream_name}.#{gen_state.consumer_name} is connecting to Gnat.",
      module: module,
      listening_topic: listening_topic,
      connection_name: connection_name
    )

    # Mint a fresh inbox on every (re)connect. Reusing the same topic across
    # reconnects lets messages from an abandoned pull on the previous
    # connection leak into the new subscription's mailbox; rotating it
    # makes those messages addressed to a topic we no longer subscribe to,
    # and the per-message topic guard in handle_info/2 will drop any that
    # do still arrive (e.g. delivered locally before unsub propagated).
    listening_topic =
      Util.reply_inbox(gen_state.connection_options.inbox_prefix)

    with {:ok, conn} <- connection_pid(connection_name),
         monitor_ref = Process.monitor(conn),
         {:ok, consumer_info} <-
           ensure_consumer_exists(
             conn,
             stream_name,
             consumer_name,
             consumer,
             domain
           ),
         :ok <- validate_batch_ack_policy(gen_state.connection_options, consumer_info),
         final_consumer_name = consumer_info.name,
         state = maybe_handle_connected(module, consumer_info, gen_state.state),
         {:ok, sid} <- Gnat.sub(conn, self(), listening_topic),
         gen_state = %{
           gen_state
           | subscription_id: sid,
             connection_pid: conn,
             connection_monitor_ref: monitor_ref,
             consumer_name: final_consumer_name,
             listening_topic: listening_topic,
             state: state
         },
         :ok <- initial_fetch(gen_state, conn),
         gen_state = %{gen_state | current_retry: 0},
         gen_state = touch_response(gen_state) do
      {:ok, gen_state}
    else
      {:error, reason} ->
        if gen_state.current_retry >= connection_retries do
          Logger.error(
            """
            #{__MODULE__} for #{stream_name}.#{gen_state.consumer_name} failed to connect to NATS and \
            retries limit has been exhausted. Stopping.
            """,
            module: module,
            listening_topic: listening_topic,
            connection_name: connection_name
          )

          {:stop, :timeout, %{gen_state | current_retry: 0}}
        else
          Logger.debug(
            """
            #{__MODULE__} for #{stream_name}.#{gen_state.consumer_name} failed to connect to Gnat \
            and will retry. Reason: #{inspect(reason)}
            """,
            module: module,
            listening_topic: listening_topic,
            connection_name: connection_name
          )

          gen_state = Map.update!(gen_state, :current_retry, &(&1 + 1))
          {:backoff, connection_retry_timeout, gen_state}
        end
    end
  end

  def disconnect(
        {:close, from},
        %__MODULE__{
          connection_options: %ConnectionOptions{
            stream_name: stream_name,
            connection_name: connection_name
          },
          listening_topic: listening_topic,
          subscription_id: subscription_id,
          module: module,
          consumer_name: consumer_name
        } = gen_state
      ) do
    Logger.debug(
      "#{__MODULE__} for #{stream_name}.#{consumer_name} is disconnecting from Gnat.",
      module: module,
      listening_topic: listening_topic,
      subscription_id: subscription_id,
      connection_name: connection_name
    )

    with {:ok, conn} <- connection_pid(connection_name),
         true <- Process.demonitor(gen_state.connection_monitor_ref, [:flush]),
         :ok <- Gnat.unsub(conn, subscription_id) do
      Logger.debug(
        "#{__MODULE__} for #{stream_name}.#{consumer_name} is shutting down.",
        module: module,
        listening_topic: listening_topic,
        subscription_id: subscription_id,
        connection_name: connection_name
      )

      Connection.reply(from, :ok)
      {:stop, :shutdown, gen_state}
    end
  end

  defp ensure_consumer_exists(gnat, stream_name, consumer_name, nil, domain) do
    # Durable consumer case - just check it exists
    try do
      case Gnat.Jetstream.API.Consumer.info(gnat, stream_name, consumer_name, domain) do
        {:ok, consumer_info} -> {:ok, consumer_info}
        {:error, reason} -> {:error, reason}
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
      :all ->
        :ok

      other ->
        {:error,
         "batch_size > 1 requires ack_policy: :all on the consumer, " <>
           "got: #{inspect(other)}. With ack_policy: :explicit, " <>
           "only the last message in each batch would be acknowledged and the " <>
           "server would redeliver the rest"}
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
      %{gen_state | state: new_state}
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

  # -- Drop messages from a stale Gnat subscription. After a reconnect we
  # mint a fresh inbox + subscription id (sid), but messages already in
  # flight to the prior subscription can still arrive locally. Acking,
  # processing, or re-pulling on those would corrupt the new subscription's
  # state, so we log and drop.
  #
  # Gnat's :msg struct doesn't carry the inbox we subscribed on — only the
  # original publish topic, the connection pid, and an integer sid. So we
  # identify the current subscription by the (gnat_pid, sid) pair. This
  # tuple is unique even across Gnat process restarts: a new Gnat process
  # has a different pid, so its first sid=1 won't collide with the old
  # process's sid=1. --
  def handle_info(
        {:msg, %{gnat: msg_gnat, sid: msg_sid} = message},
        %__MODULE__{connection_pid: conn, subscription_id: sid} = gen_state
      )
      when is_integer(sid) and (msg_gnat != conn or msg_sid != sid) do
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
        request_batch(gnat, gen_state, :tailing)
        {:noreply, gen_state}

      _messages ->
        # Partial batch — process what we have, then try for more.
        gen_state = process_and_ack_batch(gen_state)
        request_batch(gnat, gen_state, :catching_up)
        {:noreply, gen_state}
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

    next_message(message.gnat, gen_state)

    {:noreply, gen_state}
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
      gen_state = process_and_ack_batch(gen_state)
      request_batch(message.gnat, gen_state, :catching_up)
      {:noreply, gen_state}
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
            connection_name: connection_name
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

    case module.handle_message(message, state) do
      {:ack, state} ->
        Gnat.Jetstream.ack_next(message, listening_topic)

        gen_state = %{gen_state | state: state}
        {:noreply, gen_state}

      {:nack, state} ->
        Gnat.Jetstream.nack(message)
        next_message(message.gnat, gen_state)
        gen_state = %{gen_state | state: state}
        {:noreply, gen_state}

      {:term, state} ->
        Gnat.Jetstream.ack_term(message)
        next_message(message.gnat, gen_state)
        gen_state = %{gen_state | state: state}
        {:noreply, gen_state}

      {:noreply, state} ->
        next_message(message.gnat, gen_state)
        gen_state = %{gen_state | state: state}
        {:noreply, gen_state}
    end
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

    {:connect, :reconnect, reset_to_disconnected(gen_state)}
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
        {:connect, :heartbeat_expired, reset_to_disconnected(gen_state)}
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

  def handle_call(
        :close,
        from,
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
    Logger.debug("#{__MODULE__} for #{stream_name}.#{consumer_name} received :close call.",
      module: module,
      listening_topic: listening_topic,
      subscription_id: subscription_id,
      connection_name: connection_name
    )

    {:disconnect, {:close, from}, gen_state}
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
    Gnat.Jetstream.API.Consumer.request_next_message(
      conn,
      stream_name,
      consumer_name,
      listening_topic,
      domain,
      expires: expires,
      idle_heartbeat: idle_heartbeat
    )
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

    Gnat.Jetstream.API.Consumer.request_next_message(
      conn,
      stream_name,
      consumer_name,
      listening_topic,
      domain,
      opts
    )
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

  # Tear down everything tied to the current Gnat connection and reset the
  # corresponding fields in gen_state. Side effects and state mutation live
  # together so callers don't have to remember to do both.
  #
  # Safe to call from any path:
  #   * Heartbeat-expired reconnect: connection still alive, demonitor +
  #     unsub do real work.
  #   * :DOWN handler: monitor already fired (demonitor is a no-op) and the
  #     Gnat pid is gone (connection_pid returns :not_found, unsub skipped).
  defp reset_to_disconnected(%__MODULE__{} = gen_state) do
    # Always flush any buffered messages through the user's handler
    # before tearing down. The handler invocation is pure consumer-side
    # work and is always safe; the ack is best-effort against the
    # original Gnat pid. If the ack fails, the server will redeliver
    # after ack_wait and at-least-once is preserved.
    #
    # Why we MUST do this for batch mode: under ack_policy: :all,
    # acking message N moves the consumer's ack_floor to N, covering
    # everything ≤ N. If we silently drop a partial batch [101..115]
    # and a later batch's ack on seq 125 arrives, the server treats
    # 101..115 as acked and never redelivers them — a true loss.
    gen_state =
      if gen_state.buffer == [] do
        gen_state
      else
        Logger.info(
          "[#{__MODULE__}] flushing #{length(gen_state.buffer)} buffered messages " <>
            "before reconnect for #{gen_state.connection_options.stream_name}.#{gen_state.consumer_name}"
        )

        try do
          process_and_ack_batch(gen_state)
        catch
          :exit, reason ->
            # Ack publish failed (Gnat pid is dead, dying, or wedged).
            # The user's handler already ran for these messages, and
            # the server will redeliver after ack_wait — both halves
            # of at-least-once are covered.
            Logger.info(
              "[#{__MODULE__}] ack of flushed batch failed (#{inspect(reason)}); " <>
                "messages will be redelivered after ack_wait"
            )

            %{gen_state | buffer: []}
        end
      end

    if gen_state.connection_monitor_ref do
      Process.demonitor(gen_state.connection_monitor_ref, [:flush])
    end

    if gen_state.subscription_id && is_pid(gen_state.connection_pid) do
      # Best-effort unsub against the same Gnat that owns the sid (the
      # pid we stored on the successful sub, not Process.whereis(name)
      # which could resolve to a fresh-but-wedged Gnat after a restart).
      # try/catch covers a dead pid or a slow GenServer.call.
      try do
        _ = Gnat.unsub(gen_state.connection_pid, gen_state.subscription_id)
      catch
        :exit, _ -> :ok
      end
    end

    %{
      gen_state
      | consumer_name: nil,
        subscription_id: nil,
        connection_pid: nil,
        connection_monitor_ref: nil,
        listening_topic: nil,
        buffer: [],
        last_response_at: nil
    }
  end

  defp process_and_ack_batch(%{buffer: buffer, module: module, state: state} = gen_state) do
    messages = Enum.reverse(buffer)

    new_state =
      Enum.reduce(messages, state, fn message, acc_state ->
        case module.handle_message(message, acc_state) do
          {:ack, updated_state} ->
            updated_state

          {action, updated_state} ->
            Logger.warning(
              "PullConsumer batch mode does not support #{inspect(action)}, treating as :ack"
            )

            updated_state
        end
      end)

    # With ack_policy: :all, acking the last message covers the entire batch
    last = List.last(messages)
    Gnat.Jetstream.ack(last)

    %{gen_state | state: new_state, buffer: []}
  end
end
