defmodule Gnat.Services.ServiceResponder do
  @moduledoc false

  require Logger
  alias Gnat.Services.WireProtocol
  @subscription_subject "$SRV.>"
  @op_ping "PING"
  @op_stats "STATS"
  @op_info "INFO"

  @idx_requests 1
  @idx_errors 2
  @idx_processing_time 3

  @name_regex ~r/^[a-zA-Z0-9_-]+$/
  @version_regex ~r/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/

  use GenServer


  @type state :: %{
    config: Gnat.Services.Server.service_configuration(),
    subject_map: map(),
    connection_pid: pid,
    instance_id: String.t,
    subscription: non_neg_integer | String.t,
    started: String.t
  }

  @spec start_link(map(), keyword()) :: GenServer.on_start
  def start_link(settings, options \\ []) do
    GenServer.start_link(__MODULE__, settings, options)
  end

  @impl GenServer
  @spec init(map) :: {:ok, state()} | {:stop, String.t}
  def init(settings) do
    Process.flag(:trap_exit, true)

    case validate_configuration(Map.get(settings, :service_config)) do
      {:error, errors} ->
        {:stop, "Invalid service configuration: #{Enum.join(errors, ",")}"}
      _ ->
        state = %{
          config: settings.service_config,
          subject_map: build_subject_map(settings.service_config.endpoints),
          connection_pid: Map.get(settings, :connection_pid),
          instance_id: :crypto.strong_rand_bytes(12) |> Base.encode64,
          subscription: nil,
          started: DateTime.to_iso8601(DateTime.utc_now())
        }

        {:ok, subscription} = case get_in(state, [:config, :queue_group]) do
          nil -> Gnat.sub(state.connection_pid, self(), @subscription_subject)
          queue_group -> Gnat.sub(state.connection_pid, self(), @subscription_subject, queue_group: queue_group)
        end

        {:ok, %{ state | subscription: subscription }}
      end

  end

  @impl true
  def handle_info({:msg, %{topic: topic, body: _body, reply_to: rt, gnat: gnat}}, state) do
    tokens = String.split(topic, ".")
    if length(tokens) >= 2 do
      operation = Enum.at(tokens, 1)
      tail = tokens |> Enum.slice(2, length(tokens)-2)
      case operation do
        @op_ping -> handle_ping(tail, state, rt, gnat)
        @op_info -> handle_service_info(tail, state, rt, gnat)
        @op_stats -> handle_stats(tail, state, rt, gnat)
      end
    end

    {:noreply, state}
  end


  @impl GenServer
  def terminate(:shutdown, state) do
    Logger.info "#{__MODULE__} starting graceful shutdown"
    Gnat.unsub(state.connection_pid, state.subscription)
    Logger.info "#{__MODULE__} finished graceful shutdown"
  end
  def terminate(reason, _state) do
    Logger.error "#{__MODULE__} unexpected shutdown #{inspect reason}"
  end

  def record_request(pid, subject, elapsed_ns) when is_pid(pid), do: GenServer.cast(pid, {:record_request, subject, elapsed_ns})
  def record_request(_, _,_), do: :ok

  def record_error(pid, subject, elapsed_ns, msg) when is_pid(pid), do: GenServer.cast(pid, {:record_error, subject, elapsed_ns, msg})
  def record_error(_,_,_,_), do: :ok

  def lookup_endpoint(pid, subject) when is_pid(pid), do: GenServer.call(pid, {:lookup_endpoint, subject})

  @impl true
  def handle_cast({:record_request, subject, elapsed_ns}, state) do
    counters = case :ets.lookup(:endpoint_stats, subject) do
      [{^subject, counters, _last_error}] ->
        counters
      [] ->
        c = :counters.new(3, [:atomics])
        :ets.insert(:endpoint_stats, {subject, c, nil})
        c
    end
    :counters.add(counters, @idx_requests, 1)
    :counters.add(counters, @idx_processing_time, elapsed_ns)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:record_error, subject, elapsed_ns, msg}, state) do
    counters = case :ets.lookup(:endpoint_stats, subject) do
      [{^subject, counters, _last_error}] ->
        counters
      [] ->
        c = :counters.new(3, [:atomics])
        :ets.insert(:endpoint_stats, {subject, c, nil})
        c
    end
    :counters.add(counters, @idx_errors, 1)
    :counters.add(counters, @idx_processing_time, elapsed_ns)
    :ets.insert(:endpoint_stats, {subject, counters, msg})

    {:noreply, state}
  end

  @impl true
  def handle_call({:lookup_endpoint, subject}, _from, state) do
    res = case Map.get(state.subject_map, subject) do
      nil ->
        {nil, nil}
      {epname, groupname} ->
        {epname, groupname}
    end
    {:reply, res, state}
  end

  defp handle_ping(tail, state, rt, gnat) do
    if should_respond(tail, state.config.name, state.instance_id) do
      output = %WireProtocol.PingResponse{
        name: state.config.name,
        id: state.instance_id,
        version: state.config.version,
        metadata: get_in(state, [:config, :metadata])
      }

      Gnat.pub(gnat, rt, Jason.encode!(output))
    end
  end

  defp handle_service_info(tail, state, rt, gnat) do
    if should_respond(tail, state.config.name, state.instance_id) do
      output = %WireProtocol.InfoResponse{
        name: state.config.name,
        description: state.config.description,
        id: state.instance_id,
        version: state.config.version,
        endpoints: state.config.endpoints |> Enum.map(fn ep ->
          %{
            name: ep.name,
            subject: ep.subject,
            metadata: Map.get(ep, :metadata, %{}),
            queue_group: Map.get(ep, :queue_group)
          }
        end)
      }

      Gnat.pub(gnat, rt, Jason.encode!(output))
    end

  end

  defp handle_stats(tail, state, rt, gnat) do
    if should_respond(tail, state.config.name, state.instance_id) do
      output = %WireProtocol.StatsResponse{
        name: state.config.name,
        id: state.instance_id,
        version: state.config.version,
        started: state.started,
        endpoints: Enum.map(state.config.endpoints, fn(ep) ->
          effective_subject = derive_subscription_subject(ep)
          {counters, last_error} = case :ets.lookup(:endpoint_stats, effective_subject) do
            [{^effective_subject, counters, last_error}] ->
              {counters, last_error}
            [] ->
              {:counters.new(3, [:atomics]), nil}
          end
          processing_time = :counters.get(counters, @idx_processing_time)
          num_errors = :counters.get(counters, @idx_errors)
          num_requests = :counters.get(counters, @idx_requests)
          total_calls = num_errors + num_requests
          avg = if total_calls > 0 do
            trunc(ceil(processing_time / total_calls))
          else
            0
          end

          %{
            name: ep.name,
            subject: Map.get(ep, :subject),
            num_requests: num_requests,
            num_errors: num_errors,
            last_error: last_error,
            processing_time: processing_time,
            average_processing_time: avg,
            queue_group: Map.get(ep, :queue_group),
            data: %{}
          }
        end)
      }

      Gnat.pub(gnat, rt, Jason.encode!(output))
    end
  end

  @spec should_respond(list, String.t, String.t) :: boolean()
  defp should_respond(tail, service_name, instance_id) do
    case tail do
      [] -> true
      [^service_name] -> true
      [^service_name, ^instance_id] -> true
      _ -> false
    end
  end

  def validate_configuration(configuration) when is_nil(configuration), do: {:error, ["Service definition cannot be null"]}
  def validate_configuration(configuration) when not is_map(configuration), do: {:error, ["Service definition must be a map"]}
  def validate_configuration(configuration) do
    rules = [
      {&valid_version?/1, configuration},
      {&valid_name?/1, configuration},
      {&valid_metadata?/1, Map.get(configuration, :metadata)}
    ]
    eprules = configuration.endpoints
    |> Enum.map(fn ep ->
      {&valid_endpoint?/1, ep}
    end)

    results = (rules ++ eprules) |> Enum.map(fn {pred, input} ->
      apply(pred, [input])
    end)
    {_good, bad} = Enum.split_with(results, fn e -> e == :ok end)

    if length(bad) == 0 do
      :ok
    else
      {:error, bad |> Enum.map(fn {:error, m} -> m end) |> Enum.to_list()}
    end
  end

  defp valid_version?(service_definition) do
    version = Map.get(service_definition, :version)
    if String.match?(version, @version_regex) do
      :ok
    else
      {:error, "Version '#{version}' does not conform to semver specification"}
    end
  end

  defp valid_name?(service_definition) do
    name = Map.get(service_definition, :name)
    if String.match?(name, @name_regex) do
      :ok
    else
      {:error, "Service name '#{name}' is invalid. Check for illegal characters"}
    end
  end

  defp valid_metadata?(nil), do: :ok
  defp valid_metadata?(md) do
    bads = Enum.filter(md, fn {k, v} ->
      !is_binary(k) or !is_binary(v)
    end) |> length()

    if bads == 0 do
      :ok
    else
      {:error, "At least one key or value found in metadata that was not a string"}
    end

  end

  defp valid_endpoint?(endpoint_definition) do
    name = Map.get(endpoint_definition, :name)
    with true <- String.match?(name, @name_regex),
      :ok <- valid_metadata?(Map.get(endpoint_definition, :metadata)) do
        :ok
      else
        false ->
          {:error, "Endpoint name '#{name}' is not valid"}
        e -> e
      end
  end

  @spec derive_subscription_subject(Gnat.Services.Server.endpoint_configuration()) :: String.t
  def derive_subscription_subject(endpoint) do
    group_prefix = case Map.get(endpoint, :group_name) do
      nil -> ""
      prefix -> "#{prefix}."
    end
    subject = case Map.get(endpoint, :subject) do
      nil -> endpoint.name
      sub -> sub
    end
    "#{group_prefix}#{subject}"
  end

  defp build_subject_map(endpoints) do
    endpoints |> Enum.map(fn ep ->
      { derive_subscription_subject(ep), {ep.name, ep.group_name}}
    end) |> Map.new()
  end
end
