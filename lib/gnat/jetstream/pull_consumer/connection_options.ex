defmodule Gnat.Jetstream.PullConsumer.ConnectionOptions do
  @moduledoc false

  @default_retry_timeout 1000
  @default_retries 10

  @enforce_keys [
    :connection_name,
    :stream_name,
    :connection_retry_timeout,
    :connection_retries,
    :inbox_prefix,
    :domain
  ]

  defstruct @enforce_keys ++ [:consumer_name, :consumer_definition]

  def validate!(connection_options) do
    validated_opts = Keyword.validate!(connection_options, [
      :connection_name,
      :stream_name,
      :consumer_name,
      :consumer_definition,
      connection_retry_timeout: @default_retry_timeout,
      connection_retries: @default_retries,
      inbox_prefix: nil,
      domain: nil
    ])

    consumer_name = validated_opts[:consumer_name]
    consumer_definition = validated_opts[:consumer_definition]

    cond do
      consumer_name && consumer_definition ->
        raise ArgumentError, "cannot specify both :consumer_name and :consumer_definition"

      !consumer_name && !consumer_definition ->
        raise ArgumentError, "must specify either :consumer_name or :consumer_definition"

      consumer_definition && !is_function(consumer_definition, 0) ->
        raise ArgumentError, ":consumer_definition must be a 0-arity function that returns a Consumer struct"

      true ->
        struct!(__MODULE__, validated_opts)
    end
  end
end
