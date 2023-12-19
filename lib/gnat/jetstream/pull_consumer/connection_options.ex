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

  # Remove this hackery when we will support Elixir ~> 1.13 only.
  if Kernel.function_exported?(Keyword, :validate!, 2) do
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
  else
    def validate!(connection_options) do
      struct!(
        __MODULE__,
        Keyword.merge(
          [
            connection_retry_timeout: @default_retry_timeout,
            connection_retries: @default_retries,
            inbox_prefix: nil,
            domain: nil
          ],
          connection_options
        )
      )
    end
  end
end
