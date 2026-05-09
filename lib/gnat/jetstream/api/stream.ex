defmodule Gnat.Jetstream.API.Stream do
  @moduledoc """
  A module representing a NATS JetStream Stream.

  Learn more about Streams: https://docs.nats.io/nats-concepts/jetstream/streams

  ## The Jetstream.API.Stream struct

  The struct's mandatory fields are `:name` and `:subjects`. Any field left unset (`nil`)
  is omitted from the request body, so the nats-server applies its own default. This keeps
  the client compatible with both older servers that recognize legacy fields and newer
  servers that have removed them.

  Stream struct fields explanation:

  * `:allow_direct` - Allow higher performance, direct access to get individual messages. E.g. KeyValue
  * `:allow_msg_ttl` - Allow header initiated per-message TTLs.
  * `:allow_rollup_hdrs` - allows the use of the Nats-Rollup header to replace all contents of a stream,
    or subject in a stream, with a single new message.
  * `:deny_delete` - restricts the ability to delete messages from a stream via the API. Cannot be changed
    once set to true.
  * `:deny_purge` - restricts the ability to purge messages from a stream via the API. Cannot be change
    once set to true.
  * `:description` - a short description of the purpose of this stream.
  * `:discard` - determines what happens when a Stream reaches its limits. It has the following options:
     - `:old` - the default option. Old messages are deleted.
     - `:new` - refuses new messages.
  * `:discard_new_per_subject` - - allows to enable discarding new messages per subject when limits are reached.
    Requires `discard: :new` and the `:max_msgs_per_subject` to be configured.
  * `:domain` - JetStream domain, mainly used for leaf nodes.
     See [JetStream on Leaf Nodes](https://docs.nats.io/running-a-nats-service/configuration/leafnodes/jetstream_leafnodes).
  * `:duplicate_window` - the window within which to track duplicate messages, expressed in nanoseconds.
  * `:max_age` - maximum age of any message in the Stream, expressed in nanoseconds.
  * `:max_bytes` - how many bytes the Stream may contain. Adheres to `:discard`, removing oldest or
    refusing new messages if the Stream exceeds this size.
  * `:max_consumers` - how many Consumers can be defined for a given Stream, -1 for unlimited.
  * `:max_msg_size` - the largest message that will be accepted by the Stream.
  * `:max_msgs_per_subject` - For wildcard streams ensure that for every unique subject this many messages are kept - a per subject retention limit.
    Only available on nats-server versions greater than 2.3.0
  * `:max_msgs` - how many messages may be in a Stream. Adheres to `:discard`, removing oldest or refusing
    new messages if the Stream exceeds this number of messages
  * `:mirror` - maintains a 1:1 mirror of another stream with name matching this property.  When a mirror
    is configured subjects and sources must be empty.
  * `:mirror_direct` - Allow higher performance and unified direct access for mirrors as well.
  * `:name` - a name for the Stream.
    See [naming](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/naming).
  * `:no_ack` - disables acknowledging messages that are received by the Stream.
  * `:num_replicas` - how many replicas to keep for each message.
  * `:placement` - placement directives to consider when placing replicas of this stream, random placement
    when unset. It has the following properties:
     - `:cluster` - the desired cluster name to place the stream.
     - `:tags` - tags required on servers hosting this stream.
  * `:retention` - how messages are retained in the Stream. Once this is exceeded, old messages are removed.
    It has the following options:
     - `:limits` - the default policy.
     - `:interest`
     - `:workqueue`
  * `:sealed` - sealed streams do not allow messages to be deleted via limits or API, sealed streams can not
    be unsealed via configuration update. Can only be set on already created streams via the Update API.
  * `:sources` - list of stream names to replicate into this stream.
  * `:storage` - the type of storage backend. Available options:
     - `:file`
     - `:memory`
  * `:compression` - If file-based and a compression algorithm is specified, the stream data will be compressed on disk.
    Valid options are "none" for no compression or "s2" for Snappy compression.
  * `:subjects` - a list of subjects to consume, supports wildcards.
  * `:subject_delete_marker_ttl` - Enables and sets a duration expressed in nanoseconds. for adding server markers for
    delete, purge and max age limits.
  * `:template_owner` - when the Stream is managed by a Stream Template this identifies the template that
    manages the Stream.
  """

  import Gnat.Jetstream.API.Util

  @enforce_keys [:name, :subjects]
  defstruct [
    :allow_direct,
    :allow_msg_ttl,
    :allow_rollup_hdrs,
    :compression,
    :deny_delete,
    :deny_purge,
    :description,
    :discard,
    :discard_new_per_subject,
    :domain,
    :duplicate_window,
    :max_age,
    :max_bytes,
    :max_consumers,
    :max_msg_size,
    :max_msgs,
    :max_msgs_per_subject,
    :mirror,
    :mirror_direct,
    :name,
    :no_ack,
    :num_replicas,
    :placement,
    :retention,
    :sealed,
    :sources,
    :storage,
    :subject_delete_marker_ttl,
    :subjects,
    :template_owner
  ]

  defimpl Jason.Encoder do
    # The Stream struct represents user intent: only fields the caller explicitly
    # set (i.e. non-nil) get sent to the server. This keeps requests forward- and
    # backward-compatible across nats-server versions that add or remove fields,
    # since nats-server uses strict JSON parsing (DisallowUnknownFields) and will
    # reject a request that contains fields it does not recognize. `:domain` is
    # part of the API subject, not the request body.
    def encode(stream, opts) do
      stream
      |> Map.from_struct()
      |> Map.drop([:domain])
      |> Map.reject(fn {_key, value} -> is_nil(value) end)
      |> Jason.Encode.map(opts)
    end
  end

  @type nanoseconds :: non_neg_integer()

  @type placement :: %{
          :cluster => binary(),
          optional(:tags) => list(binary())
        }

  @type t :: %__MODULE__{
          allow_direct: nil | boolean(),
          allow_msg_ttl: nil | boolean(),
          allow_rollup_hdrs: nil | boolean(),
          compression: nil | binary(),
          deny_delete: nil | boolean(),
          deny_purge: nil | boolean(),
          description: nil | binary(),
          discard: nil | :old | :new,
          discard_new_per_subject: nil | boolean(),
          domain: nil | binary(),
          duplicate_window: nil | nanoseconds(),
          max_age: nil | nanoseconds(),
          max_bytes: nil | integer(),
          max_consumers: nil | integer(),
          max_msg_size: nil | integer(),
          max_msgs: nil | integer(),
          max_msgs_per_subject: nil | integer(),
          mirror: nil | source(),
          mirror_direct: nil | boolean(),
          name: binary(),
          no_ack: nil | boolean(),
          num_replicas: nil | pos_integer(),
          placement: nil | placement(),
          retention: nil | :limits | :workqueue | :interest,
          sealed: nil | boolean(),
          sources: nil | list(source()),
          storage: nil | :file | :memory,
          subject_delete_marker_ttl: nil | nanoseconds(),
          subjects: nil | list(binary()),
          template_owner: nil | binary()
        }

  @typedoc """
  Stream source fields explained:

  * `:name` - stream name.
  * `:opt_start_seq` - sequence to start replicating from.
  * `:opt_start_time` - timestamp to start replicating from.
  * `:filter_subject` - replicate only a subset of messages based on filter.
  * `:external` - configuration referencing a stream source in another account or JetStream domain.
    It has the following parameters:
     - `:api` - the subject prefix that imports other account/domain `$JS.API.CONSUMER.>` subjects
     - `:deliver` - the delivery subject to use for push consumer
  """
  @type source :: %{
          :name => binary(),
          optional(:opt_start_seq) => integer(),
          optional(:opt_start_time) => DateTime.t(),
          optional(:filter_subject) => binary(),
          optional(:external) => %{
            api: binary(),
            deliver: binary()
          }
        }

  @type info :: %{
          cluster:
            nil
            | %{
                optional(:name) => binary(),
                optional(:leader) => binary(),
                optional(:replicas) =>
                  list(%{
                    :active => nanoseconds(),
                    :name => binary(),
                    :current => boolean(),
                    optional(:offline) => boolean(),
                    optional(:lag) => non_neg_integer()
                  })
              },
          config: t(),
          created: DateTime.t(),
          mirror: nil | source_info(),
          sources: nil | list(source_info()),
          state: state()
        }

  @type state :: %{
          bytes: non_neg_integer(),
          consumer_count: non_neg_integer(),
          deleted: nil | [non_neg_integer()],
          first_seq: non_neg_integer(),
          first_ts: DateTime.t(),
          last_seq: non_neg_integer(),
          last_ts: DateTime.t(),
          lost: nil | list(%{msgs: [non_neg_integer()], bytes: non_neg_integer()}),
          messages: non_neg_integer(),
          num_deleted: nil | integer(),
          num_subjects: nil | integer(),
          subjects:
            nil
            | %{}
            | %{
                binary() => non_neg_integer()
              }
        }

  @typedoc """
  * `code` - HTTP like error code in the 300 to 500 range
  * `description` - A human friendly description of the error
  * `err_code` - The NATS error code unique to each kind of error
  """
  @type response_error :: %{
          :code => non_neg_integer(),
          optional(:description) => binary(),
          optional(:err_code) => non_neg_integer()
        }

  @type source_info :: %{
          :active => nanoseconds(),
          :lag => non_neg_integer(),
          :name => binary(),
          optional(:external) => %{
            api: binary(),
            deliver: binary()
          },
          optional(:error) => response_error()
        }

  @type streams :: %{
          limit: non_neg_integer(),
          offset: non_neg_integer(),
          streams: list(binary()),
          total: non_neg_integer()
        }

  @typedoc """
  * `seq` - Stream sequence number of the message to retrieve, cannot be combined with `last_by_subj`
  * `last_by_subj` - Retrieves the last message for a given subject, cannot be combined with `seq`
  """
  @type message_access_method :: %{
          optional(:seq) => non_neg_integer(),
          optional(:last_by_subj) => binary()
        }

  @typedoc """
  * `data` - The decoded message payload
  * `subject` - The subject the message was originally received on
  * `time` - The time the message was received
  * `seq` - The sequence number of the message in the Stream
  * `hdrs` - The decoded headers for the message
  """
  @type message_response :: %{
          :data => any(),
          :seq => non_neg_integer(),
          :subject => binary(),
          :time => DateTime.t(),
          :hdrs => nil | binary()
        }

  # @doc """
  # Initialize a Stream struct

  # ## Examples

  #     iex> %Stream{} = Gnat.Jetstream.API.Stream.new(:gnat, name: "NEW_STREAM", subjects: ["NEW_STREAM.subjects"])
  # """
  # @spec new(conn :: Gnat.t(), fields :: keyword()) :: t()
  # def new(conn, fields \\ []) do
  #   %__MODULE__{}
  # end

  @doc """
  Creates a new Stream.

  ## Examples

      iex> {:ok, %{created: _}} = Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "anewstream", subjects: ["anewsubject"]})

  """
  @spec create(conn :: Gnat.t(), stream :: t()) :: {:ok, info()} | {:error, any()}
  def create(conn, %__MODULE__{} = stream) do
    with :ok <- validate(stream),
         {:ok, stream} <-
           request(
             conn,
             "#{js_api(stream.domain)}.STREAM.CREATE.#{stream.name}",
             Jason.encode!(stream)
           ) do
      {:ok, to_info(stream)}
    end
  end

  @doc """
  Updates a Stream.

  ## Examples

      iex> {:ok, %{created: _}} = Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "update_test_stream", subjects: ["update_subject"]})
      iex> {:ok, _} = Gnat.Jetstream.API.Stream.update(:gnat, %Gnat.Jetstream.API.Stream{name: "update_test_stream", subjects: ["update_subject", "new.update_subject"]})

  """
  @spec update(conn :: Gnat.t(), stream :: t()) :: {:ok, info()} | {:error, any()}
  def update(conn, %__MODULE__{} = stream) do
    with :ok <- validate(stream),
         {:ok, stream} <-
           request(
             conn,
             "#{js_api(stream.domain)}.STREAM.UPDATE.#{stream.name}",
             Jason.encode!(stream)
           ) do
      {:ok, to_info(stream)}
    end
  end

  @doc """
  Deletes a Stream and all its data.

  ## Examples

      iex> Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "delstream", subjects: ["delsubject"]})
      iex> Gnat.Jetstream.API.Stream.delete(:gnat, "delstream")
      :ok

      iex> {:error, %{"code" => 404, "description" => "stream not found"}} = Gnat.Jetstream.API.Stream.delete(:gnat, "wrong_stream")

  """
  @spec delete(conn :: Gnat.t(), stream_name :: binary(), domain :: nil | binary()) ::
          :ok | {:error, any()}
  def delete(conn, stream_name, domain \\ nil) when is_binary(stream_name) do
    with {:ok, _response} <- request(conn, "#{js_api(domain)}.STREAM.DELETE.#{stream_name}", "") do
      :ok
    end
  end

  @doc """
  Purges all of data in the stream but doesn't delete the stream.

  ## Examples

      iex> Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "purgestream", subjects: ["purgesubject"]})
      iex> Gnat.Jetstream.API.Stream.purge(:gnat, "purgestream")
      :ok

      iex> {:error, %{"code" => 404, "description" => "stream not found"}} = Gnat.Jetstream.API.Stream.purge(:gnat, "wrong_stream")

  """
  @spec purge(conn :: Gnat.t(), stream_name :: binary(), domain :: nil | binary()) ::
          :ok | {:error, any()}
  def purge(conn, stream_name, domain \\ nil) when is_binary(stream_name) do
    with {:ok, _response} <- request(conn, "#{js_api(domain)}.STREAM.PURGE.#{stream_name}", "") do
      :ok
    end
  end

  @doc """
  Purges some of the messages in a stream according to the supplied filter

  ## Examples

      iex> Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "pstream", subjects: ["psub1", "psub2"]})
      iex> Gnat.Jetstream.API.Stream.purge(:gnat, "pstream", nil, %{filter: "psub1"})
      :ok

  """
  @type method :: %{filter: String.t()}
  @spec purge(conn :: Gnat.t(), stream_name :: binary(), domain :: nil | binary(), method) ::
          :ok | {:error, any()}
  def purge(conn, stream_name, domain, method) when is_binary(stream_name) do
    with :ok <- validate_purge_method(method),
         body <- Jason.encode!(method),
         {:ok, _response} <- request(conn, "#{js_api(domain)}.STREAM.PURGE.#{stream_name}", body) do
      :ok
    end
  end

  @doc """
  Information about config and state of a Stream.

  ## Examples

      iex> {:ok, _} = Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "infostream", subjects: ["infosubject"]})
      iex> {:ok, %{created: _}} = Gnat.Jetstream.API.Stream.info(:gnat, "infostream")

      iex> {:error, %{"code" => 404, "description" => "stream not found"}} = Gnat.Jetstream.API.Stream.info(:gnat, "wrong_stream")

  """
  @spec info(conn :: Gnat.t(), stream_name :: binary(), domain :: nil | binary()) ::
          {:ok, info()} | {:error, any()}
  def info(conn, stream_name, domain \\ nil) when is_binary(stream_name) do
    with {:ok, decoded} <- request(conn, "#{js_api(domain)}.STREAM.INFO.#{stream_name}", "") do
      {:ok, to_info(decoded)}
    end
  end

  @doc """
  Paged list of known Streams including all their current information.

  ## Options

  * `:offset` - Number of records to skip
  * `:subject` - A subject the `Stream` must collect to appear in the list.

  ## Examples

      iex> {:ok, %{total: _, offset: 0, limit: 1024, streams: _}} = Gnat.Jetstream.API.Stream.list(:gnat)

  """
  @spec list(
          conn :: Gnat.t(),
          params :: [offset: non_neg_integer(), subject: binary(), domain: nil | binary()]
        ) ::
          {:ok, streams()} | {:error, term()}
  def list(conn, params \\ []) do
    domain = Keyword.get(params, :domain)

    payload =
      Jason.encode!(%{
        offset: Keyword.get(params, :offset, 0),
        subject: Keyword.get(params, :subject)
      })

    with {:ok, decoded} <- request(conn, "#{js_api(domain)}.STREAM.NAMES", payload) do
      # Recent versions of NATS sometimes return `"streams": null` in their JSON payload to indicate
      # that no streams are defined. But, that would mean callers have to handle both `nil` and a list, so
      # we coerce that to an empty list to represent no streams being defined.
      streams =
        case Map.get(decoded, "streams") do
          nil -> []
          names when is_list(names) -> names
        end

      result = %{
        limit: Map.get(decoded, "limit"),
        offset: Map.get(decoded, "offset"),
        streams: streams,
        total: Map.get(decoded, "total")
      }

      {:ok, result}
    end
  end

  @doc """
  Get a message from the stream either by "stream sequence number" or the "last message for a given subject"
  """
  @spec get_message(
          conn :: Gnat.t(),
          stream_name :: binary(),
          method :: message_access_method(),
          domain :: nil | binary()
        ) ::
          {:ok, message_response()} | {:error, response_error()}
  def get_message(conn, stream_name, method, domain \\ nil) when is_map(method) do
    with :ok <- validate_message_access_method(method),
         {:ok, %{"message" => message}} <-
           request(conn, "#{js_api(domain)}.STREAM.MSG.GET.#{stream_name}", Jason.encode!(method)) do
      {:ok,
       %{
         data: decode_base64(message["data"]),
         seq: message["seq"],
         subject: message["subject"],
         time: to_datetime(message["time"]),
         hdrs: decode_base64(message["hdrs"])
       }}
    end
  end

  # https://docs.nats.io/running-a-nats-service/configuration/leafnodes/jetstream_leafnodes
  defp js_api(nil), do: "$JS.API"
  defp js_api(""), do: "$JS.API"
  defp js_api(domain), do: "$JS.#{domain}.API"

  defp to_state(state) do
    %{
      bytes: Map.fetch!(state, "bytes"),
      consumer_count: Map.fetch!(state, "consumer_count"),
      deleted: Map.get(state, "deleted"),
      first_seq: Map.fetch!(state, "first_seq"),
      first_ts: Map.fetch!(state, "first_ts") |> to_datetime(),
      last_seq: Map.fetch!(state, "last_seq"),
      last_ts: Map.fetch!(state, "last_ts") |> to_datetime(),
      lost: Map.get(state, "lost"),
      messages: Map.fetch!(state, "messages"),
      num_deleted: Map.get(state, "num_deleted"),
      num_subjects: Map.get(state, "num_subjects"),
      subjects: Map.get(state, "subjects")
    }
  end

  defp to_stream(stream) do
    %__MODULE__{
      description: Map.get(stream, "description"),
      discard: stream |> Map.get("discard") |> to_discard(),
      duplicate_window: Map.get(stream, "duplicate_window"),
      max_age: Map.get(stream, "max_age"),
      max_bytes: Map.get(stream, "max_bytes"),
      max_consumers: Map.get(stream, "max_consumers"),
      max_msg_size: Map.get(stream, "max_msg_size"),
      max_msgs_per_subject: Map.get(stream, "max_msgs_per_subject"),
      max_msgs: Map.get(stream, "max_msgs"),
      mirror: Map.get(stream, "mirror"),
      name: Map.fetch!(stream, "name"),
      no_ack: Map.get(stream, "no_ack"),
      num_replicas: Map.get(stream, "num_replicas"),
      placement: Map.get(stream, "placement"),
      retention: stream |> Map.get("retention") |> to_retention(),
      sources: Map.get(stream, "sources"),
      storage: stream |> Map.get("storage") |> to_storage(),
      subjects: Map.get(stream, "subjects"),
      template_owner: Map.get(stream, "template_owner"),
      allow_direct: Map.get(stream, "allow_direct"),
      allow_msg_ttl: Map.get(stream, "allow_msg_ttl"),
      allow_rollup_hdrs: Map.get(stream, "allow_rollup_hdrs"),
      deny_delete: Map.get(stream, "deny_delete"),
      deny_purge: Map.get(stream, "deny_purge"),
      discard_new_per_subject: Map.get(stream, "discard_new_per_subject"),
      mirror_direct: Map.get(stream, "mirror_direct"),
      sealed: Map.get(stream, "sealed"),
      subject_delete_marker_ttl: Map.get(stream, "subject_delete_marker_ttl"),
      compression: Map.get(stream, "compression")
    }
  end

  # Explicit enum decoders. These also act as the only compile-time references
  # to the known enum atoms; without them, `String.to_existing_atom/1` would
  # fail at runtime since the struct no longer carries default atom values.
  defp to_retention(nil), do: nil
  defp to_retention("limits"), do: :limits
  defp to_retention("workqueue"), do: :workqueue
  defp to_retention("interest"), do: :interest

  defp to_storage(nil), do: nil
  defp to_storage("file"), do: :file
  defp to_storage("memory"), do: :memory

  defp to_discard(nil), do: nil
  defp to_discard("old"), do: :old
  defp to_discard("new"), do: :new

  defp to_info(%{"config" => config, "state" => state, "created" => created} = response) do
    with {:ok, created, _} <- DateTime.from_iso8601(created) do
      %{
        cluster: Map.get(response, "cluster"),
        config: to_stream(config),
        created: created,
        mirror: Map.get(response, "mirror"),
        sources: Map.get(response, "sources"),
        state: to_state(state)
      }
    end
  end

  defp validate(stream_settings) do
    cond do
      Map.has_key?(stream_settings, :name) == false ->
        {:error, "Must have a :name set"}

      is_binary(Map.get(stream_settings, :name)) == false ->
        {:error, "name must be a string"}

      valid_name?(stream_settings.name) == false ->
        {:error, "invalid name: " <> invalid_name_message()}

      Map.has_key?(stream_settings, :subjects) == false ->
        {:error, "You must specify a :subjects key"}

      is_list(Map.get(stream_settings, :subjects)) == false ->
        {:error, ":subjects must be a list of strings"}

      Enum.all?(Map.get(stream_settings, :subjects), &is_binary/1) == false ->
        {:error, ":subjects must be a list of strings"}

      true ->
        :ok
    end
  end

  defp validate_message_access_method(method) do
    if map_size(method) == 1 do
      :ok
    else
      {:error, "To get a message you must use only one of `seq` or `last_by_subj`"}
    end
  end

  defp validate_purge_method(%{filter: subject}) when is_binary(subject) do
    :ok
  end

  defp validate_purge_method(_) do
    {:error, "When purging, you must pass a %{filter: subject}"}
  end
end
