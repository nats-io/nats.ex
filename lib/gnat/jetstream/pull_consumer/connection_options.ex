defmodule Gnat.Jetstream.PullConsumer.ConnectionOptions do
  @moduledoc false

  @default_retry_timeout 1000
  @default_retries 10

  @enforce_keys [
    :connection_name,
    :stream_name,
    :consumer_name,
    :connection_retry_timeout,
    :connection_retries,
    :inbox_prefix,
    :domain
  ]

  defstruct @enforce_keys

  def validate!(connection_options) do
    struct!(
      __MODULE__,
      Keyword.validate!(connection_options, [
        :connection_name,
        :stream_name,
        :consumer_name,
        connection_retry_timeout: @default_retry_timeout,
        connection_retries: @default_retries,
        inbox_prefix: nil,
        domain: nil
      ])
    )
  end
end
