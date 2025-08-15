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

  defstruct @enforce_keys ++ [:stream_name, :consumer_name, :consumer]

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
        domain: nil
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
