defmodule Gnat.Services.Server do
  require Logger

  @moduledoc """
  A behavior for acting as a NATS service

  Creating a service with this behavior works almost exactly the same as `Gnat.Server`,
  with the bonus that this service keeps track of requests, errors, processing time, and
  participates in service discovery and monitoring as defined by
  the [NATS service protocol](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-32.md).


  ## Example

      defmodule MyApp.Service do
        use Gnat.Services.Server

        # Classic subject matching
        def request(%{body: _body, topic: "myservice.req"}, _, _) do
          {:reply, "handled request"}
        end

        # Can also match on endpoint or group
        def request(msg, "add", "calculator") do
          {:reply, "42"}
        end

        # defining an error handler is optional, the default one will just call Logger.error for you
        def error(%{gnat: gnat, reply_to: reply_to}, _error) do
          Gnat.pub(gnat, reply_to, "Something went wrong and I can't handle your request")
        end
      end
  """
alias Gnat.Services.ServiceResponder

  @doc """
  Called when a message is received from the broker. The endpoint on which the message arrived
  is always supplied. If the endpoint is a member of a group, the group name will also be
  provided.

  Automatically increments the request time and processing time stats for this service.
  """
  @callback request(message::Gnat.message(), endpoint :: String.t(), group :: String.t() | nil) :: :ok | {:reply, iodata()} | {:error, term()}

  @doc """
  Called when an error occured during the `request/1`. Automatically increments the error count
  and processing time stats for this service.

  If your `request/1` function returned `{:error, term}`, then the `term` you returned will be passed as the second argument.
  If an exception was raised during your `request/1` function, then the exception will be passed as the second argument.
  If your `request/1` function returned something other than the supported return types, then its return value will be passed as the second argument.
  """
  @callback error(message::Gnat.message(), error::term()) :: :ok | {:reply, iodata()}

  defmacro __using__(_opts) do
    quote do
      @behaviour Gnat.Services.Server

      def error(_message, error) do
        require Logger
        Logger.error(
          "Gnat.Server encountered an error while handling a request: #{inspect(error)}",
          type: :gnat_server_error,
          error: error
        )
      end

      defoverridable error: 2
    end
  end


  @typedoc """
  Service configuration is provided as part of the consumer supervisor settings in the `service_definition` field.
  You can specify _either_ the `subscription_topics` field for a reguar server or the `service_definition` field for a
  new NATS service.

  * `name` - The name of the service. Needs to conform to the rules for NATS service names
  * `version` - A required version number (w/out "v" prefix) conforming to semver rules
  * `queue_group` - An optional queue group for service subscriptions. If left off, "q" will be used.
  * `description` - An optional description of the service
  * `metadata` - An optional string->string map of service metadata
  * `endpoints` - A required list of service endpoints. All services must have at least one endpoint
  """
  @type service_configuration :: %{
    required(:name) =>         binary(),
    required(:version) =>      binary(),
    required(:endpoints) =>    [endpoint_configuration()],
    optional(:queue_group) => binary(),
    optional(:description) => binary(),
    optional(:metadata) =>     map(),
  }

  @typedoc """
  Each service configuration must contain at least one endpoint. Endpoints can manually specify their
  subscription subjects or they can be derived from the endpoint name.

  * `subject` - A specific subject for this endpoint to listen on. If this is not provided, then the endpoint name will be used.
  * `name` - The required name of the endpoint
  * `group_name` - An optional group to which this endpoint belongs
  * `queue_group` - A queue group for this endpoint's subscription. If not supplied, "q" will be used.
  * `metadata` - An optional string->string map containing metadata for this endpoint
  """
  @type endpoint_configuration :: %{
    required(:name) => binary(),
    optional(:subject) => binary(),
    optional(:group_name) => binary(),
    optional(:queue_group) => binary(),
    optional(:metadata) => map()
  }


  @doc false
  def execute(module, message, responder_pid) do
    try do
      {endpoint, group} = ServiceResponder.lookup_endpoint(responder_pid, message.topic)

      case :timer.tc(fn -> apply(module, :request, [message, endpoint, group]) end) do
        {_elapsed, :ok} -> :done
        {elapsed_micros, {:reply, data}} ->
          send_reply(message, data)
          :telemetry.execute([:gnat, :service_request], %{latency: elapsed_micros}, %{topic: message.topic, endpoint: endpoint, group: group})
          ServiceResponder.record_request(responder_pid, message.topic, elapsed_micros * 1000 )
        {elapsed_micros, {:error, error}} ->
          execute_error(module, message, error)
          :telemetry.execute([:gnat, :service_error], %{latency: elapsed_micros}, %{topic: message.topic, endpoint: endpoint, group: group})
          ServiceResponder.record_error(responder_pid, message.topic, elapsed_micros * 1000, inspect(error))
        other -> execute_error(module, message, other)
      end

    rescue e ->
      execute_error(module, message, e)
    end
  end

  @doc false
  defp execute_error(module, message, error) do
    try do
      case apply(module, :error, [message, error]) do
        :ok -> :done
        {:reply, data} -> send_reply(message, data)
        other ->
          Logger.error(
            "error handler for #{module} returned something unexpected: #{inspect(other)}",
            type: :gnat_server_error
          )
      end

    rescue e ->
      Logger.error(
        "error handler for #{module} encountered an error: #{inspect(e)}",
        type: :gnat_server_error
      )
    end
  end

  @doc false
  def send_reply(%{gnat: gnat, reply_to: return_address}, iodata) when is_binary(return_address) do
    Gnat.pub(gnat, return_address, iodata)
  end
  def send_reply(_other, _iodata) do
    Logger.error(
      "Could not send reply because no reply_to was provided with the original message",
      type: :gnat_server_error
    )
  end
end
