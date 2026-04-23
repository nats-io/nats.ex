defmodule Gnat.Jetstream.API.KV.Entry do
  @moduledoc """
  A parsed view of a single message from a Key/Value bucket's underlying stream.

  Messages delivered from a KV bucket's stream encode three different operations
  (put, delete, purge) using a combination of the `kv-operation` header, the
  `nats-marker-reason` header, and the absence of any headers. Recovering the
  original key also requires stripping the `$KV.<bucket>.` subject prefix.

  This module captures that convention in one place so both the built-in
  `Gnat.Jetstream.API.KV.Watcher` (push consumer) and user-supplied
  `Gnat.Jetstream.PullConsumer` implementations can share it.

  ## Using with a custom PullConsumer

  A common use case is hydrating a local cache from a KV bucket by driving a
  `Gnat.Jetstream.PullConsumer`. Inside `c:handle_message/2`, convert the raw
  message into an `Entry` and branch on the operation:

      defmodule MyApp.KVCache do
        use Gnat.Jetstream.PullConsumer

        alias Gnat.Jetstream.API.KV

        @bucket "my_bucket"

        @impl true
        def handle_message(message, state) do
          case KV.Entry.from_message(message, @bucket) do
            {:ok, %KV.Entry{operation: :put, key: key, value: value}} ->
              {:ack, put_in(state.cache[key], value)}

            {:ok, %KV.Entry{operation: op, key: key}} when op in [:delete, :purge] ->
              {:ack, update_in(state.cache, &Map.delete(&1, key))}

            :ignore ->
              {:ack, state}
          end
        end
      end

  The returned struct also carries the JetStream `revision` (stream sequence),
  `created` timestamp, and `delta` (`num_pending`) when the message includes
  JetStream metadata, which is useful for detecting when the consumer has
  caught up with the tail of the stream (`delta == 0`).

  ## Messages that are not KV records

  `from_message/2` returns `:ignore` when the input is not a KV record — for
  example a JetStream status message (`100` heartbeat, `404`/`408` pull
  terminator, `409` leadership change) or a message whose subject does not
  belong to the given bucket. In normal operation the `Watcher` and
  `PullConsumer` layers filter status messages out before they reach user
  code, so this is a defensive fallback rather than something consumers are
  expected to rely on.
  """

  alias Gnat.Jetstream.API.Message

  @operation_header "kv-operation"
  @operation_del "DEL"
  @operation_purge "PURGE"
  @nats_marker_reason_header "nats-marker-reason"
  @subject_prefix "$KV."

  @type operation :: :put | :delete | :purge

  @type t :: %__MODULE__{
          bucket: String.t(),
          key: String.t(),
          value: binary(),
          operation: operation(),
          revision: non_neg_integer() | nil,
          created: DateTime.t() | nil,
          delta: non_neg_integer() | nil
        }

  defstruct [:bucket, :key, :value, :operation, :revision, :created, :delta]

  @doc """
  Parse a NATS message delivered from a KV bucket's underlying stream into an
  `Entry`.

  `bucket_name` must match the bucket the message was published to; it is used
  to strip the `$KV.<bucket>.` subject prefix and recover the key.

  Returns `:ignore` if the message is not a KV record for the given bucket
  (JetStream status message, wrong subject, etc.).

  The `:revision`, `:created`, and `:delta` fields are populated when the
  message carries a JetStream `$JS.ACK...` reply subject. For messages without
  one (e.g. direct get responses), those fields are `nil`.
  """
  @spec from_message(Gnat.message(), bucket_name :: String.t()) :: {:ok, t()} | :ignore
  def from_message(message, bucket_name) do
    with false <- status_message?(message),
         {:ok, key} <- extract_key(message, bucket_name) do
      entry = %__MODULE__{
        bucket: bucket_name,
        key: key,
        value: Map.get(message, :body, ""),
        operation: operation(message)
      }

      {:ok, apply_metadata(entry, message)}
    else
      _ -> :ignore
    end
  end

  defp status_message?(%{status: status}) when is_binary(status) and status != "", do: true
  defp status_message?(_), do: false

  defp extract_key(%{topic: topic}, bucket_name) do
    prefix = @subject_prefix <> bucket_name <> "."

    if String.starts_with?(topic, prefix) do
      {:ok, binary_part(topic, byte_size(prefix), byte_size(topic) - byte_size(prefix))}
    else
      :error
    end
  end

  defp operation(%{headers: headers}) when is_list(headers) do
    Enum.find_value(headers, :put, fn
      {@operation_header, @operation_del} -> :delete
      {@operation_header, @operation_purge} -> :purge
      {@nats_marker_reason_header, _} -> :delete
      _ -> false
    end)
  end

  defp operation(_message), do: :put

  defp apply_metadata(entry, message) do
    case Message.metadata(message) do
      {:ok, metadata} ->
        %__MODULE__{
          entry
          | revision: metadata.stream_seq,
            created: metadata.timestamp,
            delta: metadata.num_pending
        }

      {:error, _} ->
        entry
    end
  end
end
