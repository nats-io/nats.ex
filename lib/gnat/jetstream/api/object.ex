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
    with {:ok, meta} <- info(conn, bucket_name, object_name),
         meta <- %Meta{meta | deleted: true},
         topic <- meta_stream_topic(bucket_name, object_name),
         {:ok, body} <- Jason.encode(meta),
         {:ok, _msg} <- Gnat.request(conn, topic, body, headers: [{"Nats-Rollup", "sub"}]) do
      filter = chunk_stream_topic(meta)
      Stream.purge(conn, stream_name(bucket_name), nil, %{filter: filter})
    end
  end

  @spec get(Gnat.t(), String.t(), String.t(), (binary -> any())) :: :ok | {:error, any}
  def get(conn, bucket_name, object_name, chunk_fun) do
    with {:ok, %{config: _stream}} <- Stream.info(conn, stream_name(bucket_name)),
         {:ok, meta} <- info(conn, bucket_name, object_name) do
      receive_chunks(conn, meta, chunk_fun)
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
          meta = json_to_meta(message.data)
          {:ok, meta}

        error ->
          error
      end
    end
  end

  @type list_option :: {:show_deleted, boolean()}
  @spec list(Gnat.t(), String.t(), list(list_option())) :: {:error, any} | {:ok, list(Meta.t())}
  def list(conn, bucket_name, options \\ []) do
    with {:ok, %{config: stream}} <- Stream.info(conn, stream_name(bucket_name)),
         topic <- Util.reply_inbox(),
         {:ok, sub} <- Gnat.sub(conn, self(), topic),
         {:ok, consumer} <-
           Consumer.create(conn, %Consumer{
             stream_name: stream.name,
             deliver_subject: topic,
             deliver_policy: :last_per_subject,
             filter_subject: meta_stream_subject(bucket_name),
             ack_policy: :none,
             max_ack_pending: nil,
             replay_policy: :instant,
             max_deliver: 1
           }),
         {:ok, messages} <- receive_all_metas(sub, consumer.num_pending) do
      :ok = Gnat.unsub(conn, sub)
      :ok = Consumer.delete(conn, stream.name, consumer.name)

      show_deleted = Keyword.get(options, :show_deleted, false)

      if show_deleted do
        {:ok, messages}
      else
        {:ok, Enum.reject(messages, &(&1.deleted == true))}
      end
    end
  end

  @spec put(Gnat.t(), String.t(), String.t(), File.io_device()) ::
          {:ok, Meta.t()} | {:error, any()}
  def put(conn, bucket_name, object_name, io) do
    nuid = Util.nuid()
    chunk_topic = chunk_stream_topic(bucket_name, nuid)

    with {:ok, %{config: _}} <- Stream.info(conn, stream_name(bucket_name)),
         :ok <- purge_prior_chunks(conn, bucket_name, object_name),
         {:ok, chunks, size, digest} <- send_chunks(conn, io, chunk_topic) do
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

      case Gnat.request(conn, topic, body, headers: [{"Nats-Rollup", "sub"}]) do
        {:ok, _} ->
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
    raw = Jason.decode!(json)

    %{
      "bucket" => bucket,
      "chunks" => chunks,
      "digest" => digest,
      "name" => name,
      "nuid" => nuid,
      "size" => size
    } = raw

    %Meta{
      bucket: bucket,
      chunks: chunks,
      digest: digest,
      deleted: Map.get(raw, "deleted", false),
      name: name,
      nuid: nuid,
      size: size
    }
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

  defp receive_all_metas(sid, num_pending, messages \\ [])

  defp receive_all_metas(_sid, 0, messages) do
    {:ok, messages}
  end

  defp receive_all_metas(sid, remaining, messages) do
    receive do
      {:msg, %{sid: ^sid, body: body}} ->
        meta = json_to_meta(body)
        receive_all_metas(sid, remaining - 1, [meta | messages])
    after
      10_000 ->
        {:error, :timeout_waiting_for_messages}
    end
  end

  defp receive_chunks(conn, %Meta{} = meta, chunk_fun) do
    topic = chunk_stream_topic(meta)
    stream = stream_name(meta.bucket)
    inbox = Util.reply_inbox()
    {:ok, sub} = Gnat.sub(conn, self(), inbox)

    {:ok, consumer} =
      Consumer.create(conn, %Consumer{
        stream_name: stream,
        deliver_subject: inbox,
        deliver_policy: :all,
        filter_subject: topic,
        ack_policy: :none,
        max_ack_pending: nil,
        replay_policy: :instant,
        max_deliver: 1
      })

    :ok = receive_chunks(sub, meta.chunks, chunk_fun)

    :ok = Gnat.unsub(conn, sub)
    :ok = Consumer.delete(conn, stream, consumer.name)
  end

  defp receive_chunks(_sub, 0, _chunk_fun) do
    :ok
  end

  defp receive_chunks(sub, remaining, chunk_fun) do
    receive do
      {:msg, %{sid: ^sub, body: body}} ->
        chunk_fun.(body)
        receive_chunks(sub, remaining - 1, chunk_fun)
    after
      10_000 ->
        {:error, :timeout_waiting_for_messages}
    end
  end

  @chunk_size 128 * 1024
  defp send_chunks(conn, io, topic) do
    sha = :crypto.hash_init(:sha256)
    size = 0
    chunks = 0
    send_chunks(conn, io, topic, sha, size, chunks)
  end

  defp send_chunks(conn, io, topic, sha, size, chunks) do
    case IO.binread(io, @chunk_size) do
      :eof ->
        sha = :crypto.hash_final(sha)
        {:ok, chunks, size, sha}

      {:error, err} ->
        {:error, err}

      bytes ->
        sha = :crypto.hash_update(sha, bytes)
        size = size + byte_size(bytes)
        chunks = chunks + 1

        case Gnat.request(conn, topic, bytes) do
          {:ok, _} ->
            send_chunks(conn, io, topic, sha, size, chunks)

          error ->
            error
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
