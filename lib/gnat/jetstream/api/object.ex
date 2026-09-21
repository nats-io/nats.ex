defmodule Gnat.Jetstream.API.Object do
  @moduledoc """
  API for interacting with the JetStream Object Store

  Learn more about Object Store: https://docs.nats.io/nats-concepts/jetstream/obj_store
  """
  alias Gnat.Jetstream.API.{Consumer, Stream, Util}
  alias Gnat.Jetstream.API.Object.Meta

  @stream_prefix "OBJ_"
  @subject_prefix "$O."

  @type bucket_opt ::
          {:description, String.t()}
          | {:max_bucket_size, integer()}
          | {:max_chunk_size, integer()}
          | {:placement, Stream.placement()}
          | {:replicas, non_neg_integer()}
          | {:storage, :file | :memory}
          | {:ttl, non_neg_integer()}
  @spec create_bucket(Gnat.t(), String.t(), list(bucket_opt)) ::
          {:ok, Stream.info()} | {:error, any()}
  def create_bucket(conn, bucket_name, params \\ []) do
    with :ok <- validate_bucket_name(bucket_name) do
      stream = %Stream{
        name: stream_name(bucket_name),
        subjects: stream_subjects(bucket_name),
        description: Keyword.get(params, :description),
        discard: :new,
        allow_rollup_hdrs: true,
        max_age: Keyword.get(params, :ttl, 0),
        max_bytes: Keyword.get(params, :max_bucket_size, -1),
        max_msg_size: Keyword.get(params, :max_chunk_size, -1),
        num_replicas: Keyword.get(params, :replicas, 1),
        storage: Keyword.get(params, :storage, :file),
        placement: Keyword.get(params, :placement),
        duplicate_window: adjust_duplicate_window(Keyword.get(params, :ttl, 0))
      }

      Stream.create(conn, stream)
    end
  end

  @spec delete_bucket(Gnat.t(), String.t()) :: :ok | {:error, any}
  def delete_bucket(conn, bucket_name) do
    Stream.delete(conn, stream_name(bucket_name))
  end

  @spec delete(Gnat.t(), String.t(), String.t()) :: :ok | {:error, any}
  def delete(conn, bucket_name, object_name) do
    with {:ok, meta = %Meta{}} <- info(conn, bucket_name, object_name),
         meta <- %Meta{meta | deleted: true},
         topic <- meta_stream_topic(bucket_name, object_name),
         {:ok, body} <- Jason.encode(meta),
         {:ok, _msg} <- Gnat.request(conn, topic, body, headers: [{"Nats-Rollup", "sub"}]) do
      filter = chunk_stream_topic(meta)
      Stream.purge(conn, stream_name(bucket_name), nil, %{filter: filter})
    end
  end

  @doc """
  Streams an object's chunks through `chunk_fun` and verifies its size and SHA-256 digest.

  Chunks reach the callback before the final integrity check. Treat the result as
  unverified until this function returns `:ok`. Callback exceptions propagate after
  subscription and consumer cleanup is attempted.

  ## Options

    * `:timeout` - (non-negative integer) the maximum wait for each object chunk,
      in milliseconds. The wait restarts after each data callback returns.
      Heartbeats and flow-control messages don't restart it. Defaults to `10_000`.

    * `:total_timeout` - (non-negative integer or `:infinity`) the total receive
      deadline, in milliseconds, starting after consumer creation. Each wait is
      limited by both this deadline and `:timeout`. Callback time counts toward
      the deadline, but callbacks aren't interrupted. Defaults to `:infinity`.
  """
  @spec get(Gnat.t(), String.t(), String.t(), (binary -> any()), keyword()) ::
          :ok | {:error, any}
  def get(conn, bucket_name, object_name, chunk_fun, opts \\ []) do
    with {:ok, %{config: _stream}} <- Stream.info(conn, stream_name(bucket_name)),
         {:ok, meta} <- info(conn, bucket_name, object_name),
         {:ok, digest} <- decode_digest(meta.digest) do
      receive_chunks(conn, meta, digest, chunk_fun, opts)
    end
  end

  @spec info(Gnat.t(), String.t(), String.t()) :: {:ok, Meta.t()} | {:error, any}
  def info(conn, bucket_name, object_name) do
    with {:ok, _stream_info} <- Stream.info(conn, stream_name(bucket_name)) do
      Stream.get_message(conn, stream_name(bucket_name), %{
        last_by_subj: meta_stream_topic(bucket_name, object_name)
      })
      |> case do
        {:ok, message} ->
          json_to_meta(message.data)

        error ->
          error
      end
    end
  end

  @type list_option ::
          {:show_deleted, boolean()}
          | {:timeout, non_neg_integer()}
          | {:total_timeout, non_neg_integer() | :infinity}

  @doc """
  Lists object metadata in a bucket.

  ## Options

    * `:show_deleted` - (boolean) whether to include deleted objects in the result.
      Defaults to `false`.

    * `:timeout` - (non-negative integer) the maximum wait for each object's metadata,
      in milliseconds. The wait restarts after each metadata message. Heartbeats and
      flow-control messages don't restart it. Defaults to `10_000`.

    * `:total_timeout` - (non-negative integer or `:infinity`) the total receive
      deadline, in milliseconds, starting after consumer creation. Each wait is
      limited by both this deadline and `:timeout`. Defaults to `:infinity`.
  """
  @spec list(Gnat.t(), String.t(), list(list_option())) :: {:error, any} | {:ok, list(Meta.t())}
  def list(conn, bucket_name, options \\ []) do
    with {:ok, %{config: stream}} <- Stream.info(conn, stream_name(bucket_name)) do
      consumer = %Consumer{
        stream_name: stream.name,
        deliver_subject: Util.reply_inbox(),
        deliver_policy: :last_per_subject,
        filter_subject: meta_stream_subject(bucket_name),
        ack_policy: :none,
        max_ack_pending: nil,
        replay_policy: :instant,
        max_deliver: 1
      }

      with {:ok, messages} <-
             with_consumer(conn, consumer, fn conn, sub, info ->
               timeouts = receive_timeouts(options)
               receive_all_metas(conn, sub, info.num_pending, timeouts, [])
             end) do
        if Keyword.get(options, :show_deleted, false) do
          {:ok, messages}
        else
          {:ok, Enum.reject(messages, & &1.deleted)}
        end
      end
    end
  end

  @spec put(Gnat.t(), String.t(), String.t(), File.io_device()) ::
          {:ok, Meta.t()} | {:error, any()}
  def put(conn, bucket_name, object_name, io) do
    nuid = Util.nuid()
    chunk_topic = chunk_stream_topic(bucket_name, nuid)

    with {:ok, %{config: config}} <- Stream.info(conn, stream_name(bucket_name)),
         :ok <- purge_prior_chunks(conn, bucket_name, object_name),
         chunk_size <- upload_chunk_size(conn, config),
         {:ok, chunks, size, digest} <-
           send_chunks(conn, io, chunk_topic, config.name, chunk_size) do
      object_meta = %Meta{
        name: object_name,
        bucket: bucket_name,
        nuid: nuid,
        size: size,
        chunks: chunks,
        digest: "SHA-256=#{Base.url_encode64(digest)}"
      }

      topic = meta_stream_topic(bucket_name, object_name)
      body = Jason.encode!(object_meta)

      case publish(conn, config.name, topic, body, headers: [{"Nats-Rollup", "sub"}]) do
        :ok ->
          {:ok, object_meta}

        error ->
          error
      end
    end
  end

  @doc """
  Returns true if the provided stream is an Object bucket, false otherwise
  ## Parameters
  * `stream_name` - the stream name to test
  """
  @spec is_object_bucket_stream?(stream_name :: binary()) :: boolean()
  def is_object_bucket_stream?(stream_name) do
    String.starts_with?(stream_name, "OBJ_")
  end

  @doc """
  Returns a list of all Object buckets
  """
  @spec list_buckets(conn :: Gnat.t()) :: {:error, term()} | {:ok, list(String.t())}
  def list_buckets(conn) do
    with {:ok, %{streams: streams}} <- Stream.list(conn) do
      stream_names =
        streams
        |> Enum.flat_map(fn bucket ->
          if is_object_bucket_stream?(bucket) do
            [bucket |> String.trim_leading(@stream_prefix)]
          else
            []
          end
        end)

      {:ok, stream_names}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp stream_name(bucket_name) do
    "#{@stream_prefix}#{bucket_name}"
  end

  defp stream_subjects(bucket_name) do
    [
      chunk_stream_subject(bucket_name),
      meta_stream_subject(bucket_name)
    ]
  end

  defp chunk_stream_subject(bucket_name) do
    "#{@subject_prefix}#{bucket_name}.C.>"
  end

  defp chunk_stream_topic(bucket_name, nuid) do
    "#{@subject_prefix}#{bucket_name}.C.#{nuid}"
  end

  defp chunk_stream_topic(%Meta{bucket: bucket, nuid: nuid}) do
    "#{@subject_prefix}#{bucket}.C.#{nuid}"
  end

  defp meta_stream_subject(bucket_name) do
    "#{@subject_prefix}#{bucket_name}.M.>"
  end

  defp meta_stream_topic(bucket_name, object_name) do
    key = Base.url_encode64(object_name)
    "#{@subject_prefix}#{bucket_name}.M.#{key}"
  end

  @two_minutes_in_nanoseconds 120_000_000_000
  # The `duplicate_window` can't be greater than the `max_age`. The default `duplicate_window`
  # is 2 minutes. We'll keep the 2 minute window UNLESS the ttl is less than 2 minutes
  defp adjust_duplicate_window(ttl) when ttl > 0 and ttl < @two_minutes_in_nanoseconds, do: ttl
  defp adjust_duplicate_window(_ttl), do: @two_minutes_in_nanoseconds

  defp json_to_meta(json) do
    with {:ok, raw} <- Jason.decode(json),
         %{
           "bucket" => bucket,
           "chunks" => chunks,
           "digest" => digest,
           "name" => name,
           "nuid" => nuid,
           "size" => size
         }
         when is_binary(bucket) and is_integer(chunks) and chunks >= 0 and
                is_binary(digest) and is_binary(name) and is_binary(nuid) and
                is_integer(size) and size >= 0 <- raw,
         deleted when is_boolean(deleted) <- Map.get(raw, "deleted", false) do
      {:ok,
       %Meta{
         bucket: bucket,
         chunks: chunks,
         digest: digest,
         deleted: deleted,
         name: name,
         nuid: nuid,
         size: size
       }}
    else
      _ -> {:error, :invalid_object_metadata}
    end
  end

  defp purge_prior_chunks(conn, bucket, name) do
    case info(conn, bucket, name) do
      {:ok, meta} ->
        Stream.purge(conn, stream_name(bucket), nil, %{filter: chunk_stream_topic(meta)})

      {:error, %{"code" => 404}} ->
        :ok

      {:error, other} ->
        {:error, other}
    end
  end

  defp receive_all_metas(_conn, _sid, 0, _timeouts, messages) do
    {:ok, messages}
  end

  defp receive_all_metas(conn, sid, remaining, timeouts, messages) do
    with {:ok, body} <- receive_data(conn, sid, timeouts),
         {:ok, meta} <- json_to_meta(body) do
      receive_all_metas(conn, sid, remaining - 1, timeouts, [meta | messages])
    end
  end

  defp receive_chunks(conn, meta, digest, chunk_fun, opts) do
    sha = :crypto.hash_init(:sha256)

    if meta.chunks == 0 do
      verify_object(meta, digest, sha, 0)
    else
      consumer = %Consumer{
        stream_name: stream_name(meta.bucket),
        deliver_subject: Util.reply_inbox(),
        deliver_policy: :all,
        filter_subject: chunk_stream_topic(meta),
        ack_policy: :none,
        max_ack_pending: nil,
        replay_policy: :instant,
        max_deliver: 1,
        flow_control: true,
        idle_heartbeat: 5_000_000_000
      }

      with_consumer(conn, consumer, fn conn, sub, _info ->
        receive_chunks(conn, sub, meta, digest, chunk_fun, receive_timeouts(opts), sha, 0)
      end)
    end
  end

  defp receive_chunks(_conn, _sub, %{chunks: 0} = meta, digest, _fun, _timeouts, sha, size) do
    verify_object(meta, digest, sha, size)
  end

  defp receive_chunks(conn, sub, meta, digest, chunk_fun, timeouts, sha, size) do
    with {:ok, body} <- receive_data(conn, sub, timeouts) do
      size = size + byte_size(body)

      if size > meta.size do
        {:error, :size_mismatch}
      else
        chunk_fun.(body)
        sha = :crypto.hash_update(sha, body)

        receive_chunks(
          conn,
          sub,
          %{meta | chunks: meta.chunks - 1},
          digest,
          chunk_fun,
          timeouts,
          sha,
          size
        )
      end
    end
  end

  defp decode_digest("SHA-256=" <> encoded) do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, digest} when byte_size(digest) == 32 -> {:ok, digest}
      _ -> {:error, :invalid_digest}
    end
  end

  defp decode_digest(_), do: {:error, :invalid_digest}

  defp verify_object(meta, digest, sha, size) do
    cond do
      size != meta.size -> {:error, :size_mismatch}
      :crypto.hash_final(sha) != digest -> {:error, :digest_mismatch}
      true -> :ok
    end
  end

  defp receive_timeouts(opts) do
    {Keyword.get(opts, :timeout, 10_000),
     receive_deadline(Keyword.get(opts, :total_timeout, :infinity))}
  end

  defp receive_deadline(:infinity), do: :infinity

  defp receive_deadline(timeout) when is_integer(timeout) and timeout >= 0 do
    System.monotonic_time(:millisecond) + timeout
  end

  defp receive_data(conn, sub, {timeout, total_deadline}) do
    deadline = receive_deadline(timeout)
    deadline = if total_deadline == :infinity, do: deadline, else: min(deadline, total_deadline)
    receive_data_until(conn, sub, deadline)
  end

  defp receive_data_until(conn, sub, deadline) do
    timeout = deadline - System.monotonic_time(:millisecond)

    if timeout <= 0 do
      {:error, :timeout_waiting_for_messages}
    else
      receive do
        {:msg, %{gnat: ^conn, sid: ^sub, status: "100"} = message} ->
          if reply = Map.get(message, :reply_to), do: Gnat.pub(conn, reply, "")
          receive_data_until(conn, sub, deadline)

        {:msg, %{gnat: ^conn, sid: ^sub, status: status} = message} when not is_nil(status) ->
          {:error, {:object_status, status, Map.get(message, :description)}}

        {:msg, %{gnat: ^conn, sid: ^sub, body: body}} ->
          {:ok, body}
      after
        timeout -> {:error, :timeout_waiting_for_messages}
      end
    end
  end

  defp with_consumer(conn, consumer, fun) do
    conn = GenServer.whereis(conn)

    with {:ok, sub} <- Gnat.sub(conn, self(), consumer.deliver_subject) do
      try do
        with {:ok, info} <- Consumer.create(conn, consumer) do
          try do
            fun.(conn, sub, info)
          after
            cleanup(fn -> Consumer.delete(conn, consumer.stream_name, info.name) end)
          end
        end
      after
        cleanup(fn -> Gnat.unsub(conn, sub) end)
        discard_messages(conn, sub)
      end
    end
  end

  defp cleanup(fun) do
    fun.()
  catch
    :exit, _ -> :ok
  end

  defp discard_messages(conn, sub) do
    receive do
      {:msg, %{gnat: ^conn, sid: ^sub}} -> discard_messages(conn, sub)
    after
      0 -> :ok
    end
  end

  @chunk_size 128 * 1024
  defp upload_chunk_size(conn, config) do
    [@chunk_size, config.max_msg_size, Gnat.server_info(conn).max_payload]
    |> Enum.filter(&(&1 > 0))
    |> Enum.min()
  end

  defp send_chunks(conn, io, topic, stream, chunk_size) do
    sha = :crypto.hash_init(:sha256)
    size = 0
    chunks = 0
    send_chunks(conn, io, topic, stream, chunk_size, sha, size, chunks)
  end

  defp send_chunks(conn, io, topic, stream, chunk_size, sha, size, chunks) do
    case IO.binread(io, chunk_size) do
      :eof ->
        sha = :crypto.hash_final(sha)
        {:ok, chunks, size, sha}

      {:error, err} ->
        {:error, err}

      bytes ->
        sha = :crypto.hash_update(sha, bytes)
        size = size + byte_size(bytes)
        chunks = chunks + 1

        case publish(conn, stream, topic, bytes) do
          :ok ->
            send_chunks(conn, io, topic, stream, chunk_size, sha, size, chunks)

          error ->
            error
        end
    end
  end

  defp publish(conn, stream, topic, body, opts \\ []) do
    with {:ok, %{body: reply}} <- Gnat.request(conn, topic, body, opts) do
      case Jason.decode(reply) do
        {:ok, %{"error" => error}} ->
          {:error, error}

        {:ok, %{"stream" => ^stream, "seq" => seq}} when is_integer(seq) and seq > 0 ->
          :ok

        _ ->
          {:error, :invalid_publish_ack}
      end
    end
  end

  defp validate_bucket_name(name) do
    case Regex.match?(~r/^[a-zA-Z0-9_-]+$/, name) do
      true -> :ok
      false -> {:error, "invalid bucket name"}
    end
  end
end
