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
    current_retry: 0
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
            consumer_definition: consumer_definition,
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
         {:ok, final_consumer_name} <- ensure_consumer_exists(conn, stream_name, consumer_name, consumer_definition, domain),
         {:ok, sid} <- Gnat.sub(conn, self(), listening_topic),
         gen_state = %{gen_state | subscription_id: sid, connection_monitor_ref: monitor_ref, consumer_name: final_consumer_name},
         :ok <- next_message(conn, stream_name, final_consumer_name, domain, listening_topic),
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
      case check_consumer_exists(gnat, stream_name, consumer_name, domain) do
        :ok -> {:ok, consumer_name}
        {:error, reason} -> {:error, reason}
      end
    catch
      :exit, reason -> {:error, {:process_exit, reason}}
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp ensure_consumer_exists(gnat, _stream_name, nil, consumer_definition_fn, _domain) do
    # Ephemeral consumer case - create it
    try do
      with {:ok, consumer_definition} <- build_and_validate_consumer_definition(consumer_definition_fn),
           {:ok, consumer_info} <- Gnat.Jetstream.API.Consumer.create(gnat, consumer_definition) do
        {:ok, consumer_info.name}
      end
    catch
      :exit, reason -> {:error, {:process_exit, reason}}
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp build_and_validate_consumer_definition(consumer_definition_fn) do
    consumer_definition = consumer_definition_fn.()

    cond do
      consumer_definition.durable_name != nil ->
        {:error, "consumer definition must be ephemeral (cannot set durable_name)"}

      consumer_definition.inactive_threshold != nil ->
        {:error, "consumer definition must be ephemeral (cannot set inactive_threshold)"}

      true ->
        {:ok, consumer_definition}
    end
  end

  defp check_consumer_exists(gnat, stream_name, consumer_name, domain) do
    case Gnat.Jetstream.API.Consumer.info(gnat, stream_name, consumer_name, domain) do
      {:ok, _consumer} ->
        :ok

      {:error, message} ->
        {:error, message}
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
            stream_name: stream_name,
            consumer_definition: consumer_definition
          },
          subscription_id: subscription_id,
          listening_topic: listening_topic,
          module: module,
          connection_monitor_ref: monitor_ref,
          consumer_name: consumer_name
        } = gen_state
      ) when ref == monitor_ref do
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

    # Clear ephemeral consumer name on reconnect so it gets recreated
    gen_state = %{gen_state | consumer_name: nil, subscription_id: nil, connection_monitor_ref: nil}

    {:connect, :reconnect, gen_state}
  end

  def handle_info(
        other,
        %__MODULE__{
          connection_options: %ConnectionOptions{
            connection_name: connection_name,
            stream_name: stream_name,
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
end
