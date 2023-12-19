defmodule Gnat.Jetstream.API.Consumer do
  @moduledoc """
  A module representing a NATS JetStream Consumer.

  Learn more about consumers: https://docs.nats.io/nats-concepts/jetstream/consumers

  ## The Jetstream.API.Consumer struct

  The struct's only mandatory field to set is the `:stream_name`. The rest will have
  the NATS default values set.

  Note that consumers are ephemeral by default. Set the `:durable_name` to make it durable.

  Consumer struct fields explanation:

  * `:stream_name` - name of a stream the consumer is pointing at.
  * `:domain` - JetStream domain the stream is on.
  * `:ack_policy` - how the messages should be acknowledged. It has the following options:
      - `:explicit` - the default policy. It means that each individual message must be acknowledged.
        It is the only allowed option for pull consumers.
      - `:none` - no need to ack messages, the server will assume ack on delivery.
      - `:all` - only the last received message needs to be acked, all the previous messages received
        are automatically acknowledged.
  * `:ack_wait` - time in nanoseconds that server will wait for an ack for any individual. If an ack
     is not received in time, the message will be redelivered.
  * `:backoff` - list of durations that represents a retry timescale for NAK'd messages or those being
     normally retried.
  * `:deliver_group` - when set, will only deliver messages to subscriptions matching that group.
  * `:deliver_policy` - specifies where in the stream it wants to start receiving messages. It has the
     following options:
       - `:all` - the default policy. The consumer will start receiving from the earliest available
         message.
       - `:last` - the consumer will start receiving messages with the last message added to the stream.
       - `:new` - the consumer will only start receiving messages that were created after the customer
         was created.
       - `:by_start_sequence` - the consumer is required to specify `:opt_start_seq`, the sequence number
         to start on. It will receive the closest available message moving forward in the sequence
         should the message specified have been removed based on the stream limit policy.
       - `:by_start_time` - the consumer will start with messages on or after this time. The consumer is
         required to specify `:opt_start_time`, the time in the stream to start at.
       - `:last_per_subject` - the consumer will start with the latest one for each filtered subject
         currently  in the stream.
  * `:deliver_subject` - the subject to deliver observed messages. Not allowed for pull subscriptions.
    A delivery subject is required for queue subscribing as it configures a subject that all the queue
    consumers should listen on.
  * `:description` - a short description of the purpose of this customer.
  * `:durable_name` - the name of the consumer, which the server will track, allowing resuming consumption
    where left off. By default, a consumer is ephemeral. To make the consumer durable, set the name.
    See [naming](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/naming).
  * `:filter_subject` - when consuming from a stream with a wildcard subject, this allows you to select
    a subset of the full wildcard subject to receive messages from.
  * `:flow_control` - when set to true, an empty message with Status header 100 and a reply subject will
    be sent. Consumers must reply to these messages to control the rate of message delivery.
  * `:headers_only` - delivers only the headers of messages in the stream and not the bodies. Additionally
    adds the Nats-Msg-Size header to indicate the size of the removed payload.
  * `:idle_heartbeat` - if set, the server will regularly send a status message to the client while there
    are  no new messages to send. This lets the client know that the JetStream service is still up and
    running, even when there is no activity on the stream. The message status header will have a code of 100.
    Unlike `:flow_control`, it will have no reply to address. It may have a description like
    "Idle Heartbeat".
  * `:inactive_threshold` - duration that instructs the server to clean up ephemeral consumers that are
    inactive for that long.
  * `:max_ack_pending` - it sets the maximum number of messages without an acknowledgement that can be
    outstanding, once this limit is reached, message delivery will be suspended. It cannot be used with
    `:ack_none` ack policy. This maximum number of pending acks applies for all the consumer's
    subscriber processes. A value of -1 means there can be any number of pending acks (i.e. no flow
    control).
  * `:max_batch` - the largest batch property that may be specified when doing a pull on a Pull consumer.
  * `:max_deliver` - the maximum number of times a specific message will be delivered. Applies to any
    message that is re-sent due to ack policy.
  * `:max_expires` - the maximum expires value that may be set when doing a pull on a Pull consumer.
  * `:max_waiting` - the number of pulls that can be outstanding on a pull consumer, pulls received after
    this is reached are ignored.
  * `:opt_start_seq` - use with `:deliver_policy` set to `:by_start_sequence`. It represents the sequence
    number to start consuming on.
  * `:opt_start_time` - use with `:deliver_policy` set to `:by_start_time`. It represents the time to start
    consuming at.
  * `:rate_limit_bps` - used to throttle the delivery of messages to the consumer, in bits per second.
  * `:replay_policy` - it applies when the `:deliver_policy` is set to `:all`, `:by_start_sequence` or
    `:by_start_time`. It has the following options:
       - `:instant` - the default policy. The messages will be pushed to the client as fast as possible.
       - `:original` - the messages in the stream will be pushed to the client at the same rate that they
         were originally received.
  * `:sample_freq` - Sets the percentage of acknowledgements that should be sampled for observability, 0-100.
    This value is a binary and for example allows both `30` and `30%` as valid values.
  """

  import Gnat.Jetstream.API.Util

  @enforce_keys [:stream_name]
  defstruct [
    :backoff,
    :deliver_group,
    :deliver_subject,
    :description,
    :domain,
    :durable_name,
    :filter_subject,
    :flow_control,
    :headers_only,
    :idle_heartbeat,
    :inactive_threshold,
    :max_batch,
    :max_expires,
    :max_waiting,
    :opt_start_seq,
    :opt_start_time,
    :rate_limit_bps,
    :sample_freq,
    :stream_name,
    ack_policy: :explicit,
    ack_wait: 30_000_000_000,
    deliver_policy: :all,
    max_ack_pending: 20_000,
    max_deliver: -1,
    replay_policy: :instant
  ]

  @type t :: %__MODULE__{
          stream_name: binary(),
          domain: nil | binary(),
          ack_policy: :none | :all | :explicit,
          ack_wait: nil | non_neg_integer(),
          backoff: nil | [non_neg_integer()],
          deliver_group: nil | binary(),
          deliver_policy:
            :all | :last | :new | :by_start_sequence | :by_start_time | :last_per_subject,
          deliver_subject: nil | binary(),
          description: nil | binary(),
          durable_name: nil | binary(),
          filter_subject: nil | binary(),
          flow_control: nil | boolean(),
          headers_only: nil | boolean(),
          idle_heartbeat: nil | non_neg_integer(),
          inactive_threshold: nil | non_neg_integer(),
          max_ack_pending: nil | integer(),
          max_batch: nil | integer(),
          max_deliver: nil | integer(),
          max_expires: nil | non_neg_integer(),
          max_waiting: nil | integer(),
          opt_start_seq: nil | non_neg_integer(),
          opt_start_time: nil | DateTime.t(),
          rate_limit_bps: nil | non_neg_integer(),
          replay_policy: :instant | :original,
          sample_freq: nil | binary()
        }

  @type info :: %{
          ack_floor: %{
            consumer_seq: non_neg_integer(),
            stream_seq: non_neg_integer()
          },
          cluster:
            nil
            | %{
                optional(:name) => binary(),
                optional(:leader) => binary(),
                optional(:replicas) => [
                  %{
                    :active => non_neg_integer(),
                    :current => boolean(),
                    :name => binary(),
                    optional(:lag) => non_neg_integer(),
                    optional(:offline) => boolean()
                  }
                ]
              },
          config: config(),
          created: DateTime.t(),
          delivered: %{
            consumer_seq: non_neg_integer(),
            stream_seq: non_neg_integer()
          },
          name: binary(),
          num_ack_pending: non_neg_integer(),
          num_pending: non_neg_integer(),
          num_redelivered: non_neg_integer(),
          num_waiting: non_neg_integer(),
          push_bound: nil | boolean(),
          stream_name: binary()
        }

  @type config :: %{
          ack_policy: :none | :all | :explicit,
          ack_wait: nil | non_neg_integer(),
          backoff: nil | [non_neg_integer()],
          deliver_group: nil | binary(),
          deliver_policy:
            :all | :last | :new | :by_start_sequence | :by_start_time | :last_per_subject,
          deliver_subject: nil | binary(),
          description: nil | binary(),
          durable_name: nil | binary(),
          filter_subject: nil | binary(),
          flow_control: nil | boolean(),
          headers_only: nil | boolean(),
          idle_heartbeat: nil | non_neg_integer(),
          inactive_threshold: nil | non_neg_integer(),
          max_ack_pending: nil | integer(),
          max_batch: nil | integer(),
          max_deliver: nil | integer(),
          max_expires: nil | non_neg_integer(),
          max_waiting: nil | integer(),
          opt_start_seq: nil | non_neg_integer(),
          opt_start_time: nil | DateTime.t(),
          rate_limit_bps: nil | non_neg_integer(),
          replay_policy: :instant | :original,
          sample_freq: nil | binary()
        }

  @type consumers :: %{
          consumers: list(binary()),
          limit: non_neg_integer(),
          offset: non_neg_integer(),
          total: non_neg_integer()
        }

  @doc """
  Creates a consumer. When consumer's `:durable_name` field is not set, the function
  creates an ephemeral consumer. Otherwise, it creates a durable consumer.

  ## Examples

      iex> {:ok, _response} = Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "stream", subjects: ["subject"]})
      iex> {:ok, %{name: "consumer", stream_name: "stream"}} = Gnat.Jetstream.API.Consumer.create(:gnat, %Gnat.Jetstream.API.Consumer{durable_name: "consumer", stream_name: "stream"})

      iex> {:ok, _response} = Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "stream", subjects: ["subject"]})
      iex> {:error, %{"description" => "consumer delivery policy is deliver by start sequence, but optional start sequence is not set"}} = Gnat.Jetstream.API.Consumer.create(:gnat, %Gnat.Jetstream.API.Consumer{durable_name: "consumer", stream_name: "stream", deliver_policy: :by_start_sequence})

  """
  @spec create(conn :: Gnat.t(), consumer :: t()) :: {:ok, info()} | {:error, term()}
  def create(conn, %__MODULE__{durable_name: name} = consumer) when not is_nil(name) do
    create_topic =
      "#{js_api(consumer.domain)}.CONSUMER.DURABLE.CREATE.#{consumer.stream_name}.#{name}"

    with :ok <- validate_durable(consumer),
         {:ok, raw_response} <- request(conn, create_topic, create_payload(consumer)) do
      {:ok, to_info(raw_response)}
    end
  end

  def create(conn, %__MODULE__{} = consumer) do
    create_topic = "#{js_api(consumer.domain)}.CONSUMER.CREATE.#{consumer.stream_name}"

    with :ok <- validate(consumer),
         {:ok, raw_response} <- request(conn, create_topic, create_payload(consumer)) do
      {:ok, to_info(raw_response)}
    end
  end

  @doc """
  Deletes a consumer.

  ## Examples

      iex> {:ok, _response} = Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "stream", subjects: ["subject"]})
      iex> {:ok, _response} = Gnat.Jetstream.API.Consumer.create(:gnat, %Gnat.Jetstream.API.Consumer{durable_name: "consumer", stream_name: "stream"})
      iex> Gnat.Jetstream.API.Consumer.delete(:gnat, "stream", "consumer")
      :ok

      iex> {:error, %{"code" => 404, "description" => "stream not found"}} = Gnat.Jetstream.API.Consumer.delete(:gnat, "wrong_stream", "consumer")

  """
  @spec delete(
          conn :: Gnat.t(),
          stream_name :: binary(),
          consumer_name :: binary(),
          domain :: nil | binary()
        ) ::
          :ok | {:error, any()}
  def delete(conn, stream_name, consumer_name, domain \\ nil) do
    topic = "#{js_api(domain)}.CONSUMER.DELETE.#{stream_name}.#{consumer_name}"

    with {:ok, _response} <- request(conn, topic, "") do
      :ok
    end
  end

  @doc """
  Information about the consumer.

  ## Examples

      iex> {:ok, _response} = Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "stream", subjects: ["subject"]})
      iex> {:ok, _response} = Gnat.Jetstream.API.Consumer.create(:gnat, %Gnat.Jetstream.API.Consumer{durable_name: "consumer", stream_name: "stream"})
      iex> {:ok, %{created: _}} = Gnat.Jetstream.API.Consumer.info(:gnat, "stream", "consumer")

      iex>  {:error, %{"code" => 404, "description" => "stream not found"}} = Gnat.Jetstream.API.Consumer.info(:gnat, "wrong_stream", "consumer")

  """
  @spec info(
          conn :: Gnat.t(),
          stream_name :: binary(),
          consumer_name :: binary(),
          domain :: nil | binary()
        ) ::
          {:ok, info()} | {:error, any()}
  def info(conn, stream_name, consumer_name, domain \\ nil) do
    topic = "#{js_api(domain)}.CONSUMER.INFO.#{stream_name}.#{consumer_name}"

    with {:ok, raw} <- request(conn, topic, "") do
      {:ok, to_info(raw)}
    end
  end

  @doc """
  Paged list of known consumers, including their current info.

  ## Examples

      iex> {:ok, _response} =  Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "stream", subjects: ["subject"]})
      iex> {:ok, %{consumers: _, limit: 1024, offset: 0, total: _}} = Gnat.Jetstream.API.Consumer.list(:gnat, "stream")

      iex> {:error, %{"code" => 404, "description" => "stream not found"}} = Gnat.Jetstream.API.Consumer.list(:gnat, "wrong_stream")

  """
  @spec list(
          conn :: Gnat.t(),
          stream_name :: binary(),
          params :: [offset: non_neg_integer(), domain: nil | binary()]
        ) ::
          {:ok, consumers()} | {:error, term()}
  def list(conn, stream_name, params \\ []) do
    domain = Keyword.get(params, :domain)

    payload =
      Jason.encode!(%{
        offset: Keyword.get(params, :offset, 0)
      })

    with {:ok, raw} <- request(conn, "#{js_api(domain)}.CONSUMER.NAMES.#{stream_name}", payload) do
      response = %{
        consumers: Map.get(raw, "consumers"),
        limit: Map.get(raw, "limit"),
        offset: Map.get(raw, "offset"),
        total: Map.get(raw, "total")
      }

      {:ok, response}
    end
  end

  @doc """
  Requests a next message from a stream to be consumed. The response (consumed message)will be sent
  on the subject given as the `reply_to` parameter.

  ## Options

  * `batch` - How many messages to receive. Messages will be sent to the `reply_to` subject
    separately. Defaults to 1.

  * `expires` - Time in nanoseconds the request will be kept in the server. Once this time passes
    a message with empty body and topic set to `reply_to` subject is sent. Useful when polling
    the server frequently and not wanting the pull requests to accumulate. By default, the pull
    request stays in the server until a message comes.

  * `no_wait` - Boolean value which indicates whether the pull request should be accumulated on
    the server. When set to true and no message is present to be consumed, a message with empty
    body and topic value set to `reply_to` is sent. Defaults to false.

  ## Example

      iex> {:ok, _response} = Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{name: "stream", subjects: ["subject"]})
      iex> {:ok, _response} = Gnat.Jetstream.API.Consumer.create(:gnat, %Gnat.Jetstream.API.Consumer{durable_name: "consumer", stream_name: "stream"})
      iex> {:ok, _sid} = Gnat.sub(:gnat, self(), "reply_subject")
      iex> :ok = Gnat.Jetstream.API.Consumer.request_next_message(:gnat, "stream", "consumer", "reply_subject")
      iex> :ok = Gnat.pub(:gnat, "subject", "message1")
      iex> assert_receive {:msg, %{body: "message1", topic: "subject"}}
  """
  @spec request_next_message(
          conn :: Gnat.t(),
          stream_name :: binary(),
          consumer_name :: binary(),
          reply_to :: String.t(),
          domain :: nil | binary(),
          opts :: keyword()
        ) :: :ok
  def request_next_message(
        conn,
        stream_name,
        consumer_name,
        reply_to,
        domain \\ nil,
        opts \\ []
      ) do
    default_payload = %{batch: 1}

    put_option_if_not_nil = fn payload, option_key ->
      if option_value = opts[option_key] do
        Map.put(payload, option_key, option_value)
      else
        payload
      end
    end

    payload =
      default_payload
      |> put_option_if_not_nil.(:batch)
      |> put_option_if_not_nil.(:no_wait)
      |> put_option_if_not_nil.(:expires)
      |> Jason.encode!()

    Gnat.pub(
      conn,
      "#{js_api(domain)}.CONSUMER.MSG.NEXT.#{stream_name}.#{consumer_name}",
      payload,
      reply_to: reply_to
    )
  end

  # https://docs.nats.io/running-a-nats-service/configuration/leafnodes/jetstream_leafnodes
  defp js_api(nil), do: "$JS.API"
  defp js_api(""), do: "$JS.API"
  defp js_api(domain), do: "$JS.#{domain}.API"

  defp create_payload(%__MODULE__{} = cons) do
    %{
      config: %{
        ack_policy: cons.ack_policy,
        ack_wait: cons.ack_wait,
        backoff: cons.backoff,
        deliver_group: cons.deliver_group,
        deliver_policy: cons.deliver_policy,
        deliver_subject: cons.deliver_subject,
        description: cons.description,
        durable_name: cons.durable_name,
        filter_subject: cons.filter_subject,
        flow_control: cons.flow_control,
        headers_only: cons.headers_only,
        idle_heartbeat: cons.idle_heartbeat,
        inactive_threshold: cons.inactive_threshold,
        max_ack_pending: cons.max_ack_pending,
        max_batch: cons.max_batch,
        max_deliver: cons.max_deliver,
        max_expires: cons.max_expires,
        max_waiting: cons.max_waiting,
        opt_start_seq: cons.opt_start_seq,
        opt_start_time: cons.opt_start_time,
        rate_limit_bps: cons.rate_limit_bps,
        replay_policy: cons.replay_policy,
        sample_freq: cons.sample_freq
      },
      stream_name: cons.stream_name
    }
    |> Jason.encode!()
  end

  defp to_config(raw) do
    %{
      ack_policy: raw |> Map.get("ack_policy") |> to_sym(),
      ack_wait: raw |> Map.get("ack_wait"),
      backoff: Map.get(raw, "backoff"),
      deliver_group: Map.get(raw, "deliver_group"),
      deliver_policy: raw |> Map.get("deliver_policy") |> to_sym(),
      deliver_subject: raw |> Map.get("deliver_subject"),
      description: Map.get(raw, "description"),
      durable_name: Map.get(raw, "durable_name"),
      filter_subject: raw |> Map.get("filter_subject"),
      flow_control: Map.get(raw, "flow_control"),
      headers_only: Map.get(raw, "headers_only"),
      idle_heartbeat: Map.get(raw, "idle_heartbeat"),
      inactive_threshold: Map.get(raw, "inactive_threshold"),
      max_ack_pending: Map.get(raw, "max_ack_pending"),
      max_batch: Map.get(raw, "max_batch"),
      max_deliver: Map.get(raw, "max_deliver"),
      max_expires: Map.get(raw, "max_expires"),
      max_waiting: Map.get(raw, "max_waiting"),
      opt_start_seq: raw |> Map.get("opt_start_seq"),
      opt_start_time: raw |> Map.get("opt_start_time") |> to_datetime(),
      rate_limit_bps: Map.get(raw, "rate_limit_bps"),
      replay_policy: raw |> Map.get("replay_policy") |> to_sym(),
      sample_freq: Map.get(raw, "sample_freq")
    }
  end

  defp to_info(raw) do
    %{
      ack_floor: %{
        consumer_seq: get_in(raw, ["ack_floor", "consumer_seq"]),
        stream_seq: get_in(raw, ["ack_floor", "stream_seq"])
      },
      cluster: Map.get(raw, "cluster"),
      config: to_config(Map.get(raw, "config")),
      created: raw |> Map.get("created") |> to_datetime(),
      delivered: %{
        consumer_seq: get_in(raw, ["delivered", "consumer_seq"]),
        stream_seq: get_in(raw, ["delivered", "stream_seq"])
      },
      name: Map.get(raw, "name"),
      num_ack_pending: Map.get(raw, "num_ack_pending"),
      num_pending: Map.get(raw, "num_pending"),
      num_redelivered: Map.get(raw, "num_redelivered"),
      num_waiting: Map.get(raw, "num_waiting"),
      push_bound: Map.get(raw, "push_bound"),
      stream_name: Map.get(raw, "stream_name")
    }
  end

  defp validate(consumer) do
    cond do
      consumer.stream_name == nil ->
        {:error, "must have a :stream_name set"}

      is_binary(consumer.stream_name) == false ->
        {:error, "stream_name must be a string"}

      valid_name?(consumer.stream_name) == false ->
        {:error, "invalid stream_name: " <> invalid_name_message()}

      true ->
        :ok
    end
  end

  defp validate_durable(consumer) do
    with :ok <- validate(consumer) do
      cond do
        is_binary(consumer.durable_name) == false ->
          {:error, "durable_name must be a string"}

        valid_name?(consumer.durable_name) == false ->
          {:error, "invalid durable_name: " <> invalid_name_message()}

        true ->
          :ok
      end
    end
  end
end
