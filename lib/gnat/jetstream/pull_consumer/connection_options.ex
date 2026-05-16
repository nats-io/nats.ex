defmodule Gnat.Jetstream.PullConsumer.ConnectionOptions do
  @moduledoc false

  @default_retry_timeout 1000
  @default_retries 10

  @enforce_keys [
    :connection_name,
    :connection_retry_timeout,
    :connection_retries,
    :inbox_prefix,
    :domain
  ]

  # 5 Seconds in nanoseconds
  @default_request_expires 5_000_000_000
  # 15 Seconds in nanoseconds — idle heartbeat sent by the server while a
  # pull request is outstanding but no messages are available.
  @default_idle_heartbeat 15_000_000_000
  # How often the local watchdog checks whether we've heard anything from
  # the server recently. Independent of (and finer-grained than) the
  # missed-heartbeat threshold itself.
  @default_heartbeat_check_interval 5_000

  defstruct @enforce_keys ++
              [
                :stream_name,
                :consumer_name,
                :consumer,
                batch_size: 1,
                request_expires: @default_request_expires,
                idle_heartbeat: @default_idle_heartbeat,
                heartbeat_check_interval: @default_heartbeat_check_interval
              ]

  def validate!(connection_options) do
    validated_opts =
      Keyword.validate!(connection_options, [
        :connection_name,
        :stream_name,
        :consumer_name,
        :consumer,
        connection_retry_timeout: @default_retry_timeout,
        connection_retries: @default_retries,
        inbox_prefix: nil,
        domain: nil,
        batch_size: 1,
        request_expires: @default_request_expires,
        idle_heartbeat: @default_idle_heartbeat,
        heartbeat_check_interval: @default_heartbeat_check_interval
      ])

    stream_name = validated_opts[:stream_name]
    consumer_name = validated_opts[:consumer_name]
    consumer = validated_opts[:consumer]

    cond do
      consumer && (stream_name || consumer_name) ->
        raise ArgumentError,
              "cannot specify :consumer with :stream_name or :consumer_name - use consumer struct's stream_name instead"

      consumer && !is_struct(consumer, Gnat.Jetstream.API.Consumer) ->
        raise ArgumentError, ":consumer must be a Consumer struct"

      consumer && consumer.durable_name != nil && consumer.inactive_threshold == nil ->
        raise ArgumentError,
              "durable consumers specified via :consumer must have inactive_threshold set for auto-cleanup"

      consumer && validated_opts[:batch_size] > 1 && consumer.ack_policy != :all ->
        raise ArgumentError,
              "batch_size > 1 requires ack_policy: :all on the consumer, " <>
                "got: #{inspect(consumer.ack_policy)}. With ack_policy: :explicit, " <>
                "only the last message in each batch would be acknowledged and the " <>
                "server would redeliver the rest"

      consumer ->
        # For ephemeral/auto-cleanup consumer case, extract stream_name from consumer struct
        validated_opts = Keyword.put(validated_opts, :stream_name, consumer.stream_name)
        struct!(__MODULE__, validated_opts)

      stream_name && consumer_name ->
        # For traditional durable consumer case
        struct!(__MODULE__, validated_opts)

      true ->
        raise ArgumentError,
              "must specify either :consumer (ephemeral/auto-cleanup) or both :stream_name and :consumer_name (durable)"
    end
  end
end
