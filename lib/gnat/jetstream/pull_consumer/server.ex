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
    :connection_monitor_ref,
    :consumer_name,
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
             connection_monitor_ref: monitor_ref,
             consumer_name: final_consumer_name,
             state: state
         },
         :ok <-
           initial_fetch(
             gen_state,
             conn,
             stream_name,
             final_consumer_name,
             domain,
             listening_topic
           ),
         gen_state = %{gen_state | current_retry: 0} do
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

  # -- Batch mode: 100 is an idle heartbeat — the pull is still alive, do
  # nothing but invoke the user callback. --
  def handle_info(
        {:msg, %{status: "100"} = message},
        %__MODULE__{connection_options: %ConnectionOptions{batch_size: batch_size}} = gen_state
      )
      when batch_size > 1 do
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
        %__MODULE__{
          connection_options: %ConnectionOptions{
            stream_name: stream_name,
            domain: domain
          },
          listening_topic: listening_topic,
          consumer_name: consumer_name
        } = gen_state
      )
      when is_binary(status) and status != "" do
    gen_state = maybe_handle_status(message, gen_state)

    next_message(
      message.gnat,
      stream_name,
      consumer_name,
      domain,
      listening_topic
    )

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
            connection_name: connection_name,
            domain: domain
          },
          listening_topic: listening_topic,
          subscription_id: subscription_id,
          state: state,
          module: module,
          consumer_name: consumer_name
        } = gen_state
      ) do
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

        next_message(
          message.gnat,
          stream_name,
          consumer_name,
          domain,
          listening_topic
        )

        gen_state = %{gen_state | state: state}
        {:noreply, gen_state}

      {:term, state} ->
        Gnat.Jetstream.ack_term(message)

        next_message(
          message.gnat,
          stream_name,
          consumer_name,
          domain,
          listening_topic
        )

        gen_state = %{gen_state | state: state}
        {:noreply, gen_state}

      {:noreply, state} ->
        next_message(
          message.gnat,
          stream_name,
          consumer_name,
          domain,
          listening_topic
        )

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

    # Clear consumer name on reconnect so it gets recreated (for ephemeral and auto-cleanup consumers)
    # Clear buffer to avoid processing stale messages from the dead connection
    gen_state = %{
      gen_state
      | consumer_name: nil,
        subscription_id: nil,
        connection_monitor_ref: nil,
        buffer: []
    }

    {:connect, :reconnect, gen_state}
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

  defp next_message(conn, stream_name, consumer_name, domain, listening_topic) do
    Gnat.Jetstream.API.Consumer.request_next_message(
      conn,
      stream_name,
      consumer_name,
      listening_topic,
      domain
    )
  end

  defp initial_fetch(gen_state, conn, stream_name, consumer_name, domain, listening_topic) do
    if gen_state.connection_options.batch_size > 1 do
      request_batch(conn, gen_state, :catching_up)
    else
      next_message(conn, stream_name, consumer_name, domain, listening_topic)
    end
  end

  defp request_batch(conn, gen_state, mode) do
    %{
      connection_options: %ConnectionOptions{
        stream_name: stream_name,
        batch_size: batch_size,
        domain: domain,
        request_expires: expires
      },
      consumer_name: consumer_name,
      listening_topic: listening_topic
    } = gen_state

    opts =
      case mode do
        :catching_up -> [batch: batch_size, no_wait: true]
        :tailing -> [batch: batch_size, expires: expires]
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
