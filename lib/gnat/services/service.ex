defmodule Gnat.Services.Service do
  @moduledoc false

  @subscription_subject "$SRV.>"
  # required default, see https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-32.md#request-handling
  @default_service_queue_group "q"

  @idx_requests 1
  @idx_errors 2
  @idx_processing_time 3

  @name_regex ~r/^[a-zA-Z0-9_-]+$/
  @version_regex ~r/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/

  alias Gnat.Services.WireProtocol

  def init(configuration) do
    with :ok <- validate_configuration(configuration) do
      service = %{
        name: configuration.name,
        instance_id: :crypto.strong_rand_bytes(12) |> Base.encode64(),
        description: configuration.description,
        version: configuration.version,
        subjects: build_subject_map(configuration.endpoints),
        started: DateTime.to_iso8601(DateTime.utc_now()),
        metadata: Map.get(configuration, :metadata, %{})
      }

      {:ok, service}
    end
  end

  def info(service) do
    endpoint_info =
      service.subjects
      |> Map.values()
      |> Enum.map(&endpoint_info/1)

    %WireProtocol.InfoResponse{
      name: service.name,
      description: service.description,
      id: service.instance_id,
      version: service.version,
      metadata: service.metadata,
      endpoints: endpoint_info
    }
  end

  def ping(service) do
    %WireProtocol.PingResponse{
      name: service.name,
      id: service.instance_id,
      version: service.version,
      metadata: service.metadata
    }
  end

  def record_request(%{counters: counters} = _endpoint, elapsed_micros) do
    :counters.add(counters, @idx_requests, 1)
    :counters.add(counters, @idx_processing_time, elapsed_micros)
  end

  def record_error(%{counters: counters} = _endpoint, elapsed_micros) do
    :counters.add(counters, @idx_errors, 1)
    :counters.add(counters, @idx_processing_time, elapsed_micros)
  end

  def subscription_topics_with_queue_group(service) do
    endpoint_subscriptions =
      service.subjects
      |> Enum.map(fn {topic, metadata} ->
        {topic, metadata.queue_group}
      end)

    services_subscription = {@subscription_subject, nil}
    [services_subscription | endpoint_subscriptions]
  end

  def stats(service) do
    endpoint_stats =
      service.subjects
      |> Map.values()
      |> Enum.map(&endpoint_stats/1)

    %WireProtocol.StatsResponse{
      name: service.name,
      id: service.instance_id,
      version: service.version,
      started: service.started,
      endpoints: endpoint_stats
    }
  end

  defp build_subject_map(endpoints) do
    Enum.reduce(endpoints, %{}, fn ep, map ->
      subject = derive_subscription_subject(ep)

      endpoint = %{
        name: ep.name,
        queue_group: Map.get(ep, :queue_group, @default_service_queue_group),
        group_name: Map.get(ep, :group_name, nil),
        metadata: Map.get(ep, :metadata, %{}),
        subject: subject,
        counters: :counters.new(3, [:atomics])
      }

      Map.put(map, subject, endpoint)
    end)
  end

  @spec derive_subscription_subject(Gnat.Services.Server.endpoint_configuration()) :: String.t()
  defp derive_subscription_subject(endpoint) do
    group_prefix =
      case Map.get(endpoint, :group_name) do
        nil -> ""
        prefix -> "#{prefix}."
      end

    subject =
      case Map.get(endpoint, :subject) do
        nil -> endpoint.name
        sub -> sub
      end

    "#{group_prefix}#{subject}"
  end

  defp endpoint_info(endpoint) do
    %{
      name: endpoint.name,
      subject: endpoint.subject,
      metadata: endpoint.metadata,
      queue_group: endpoint.queue_group
    }
  end

  defp endpoint_stats(%{counters: counters} = endpoint) do
    micros = :counters.get(counters, @idx_processing_time)
    nanos = 1000 * micros
    num_errors = :counters.get(counters, @idx_errors)
    num_requests = :counters.get(counters, @idx_requests)
    total_calls = num_errors + num_requests

    avg =
      if total_calls > 0 do
        trunc(ceil(nanos / total_calls))
      else
        0
      end

    %{
      name: endpoint.name,
      subject: endpoint.subject,
      num_requests: num_requests,
      num_errors: num_errors,
      processing_time: nanos,
      average_processing_time: avg,
      queue_group: endpoint.queue_group
    }
  end

  defp validate_configuration(configuration) when is_nil(configuration),
    do: {:error, ["Service definition cannot be null"]}

  defp validate_configuration(configuration) when not is_map(configuration),
    do: {:error, ["Service definition must be a map"]}

  defp validate_configuration(configuration) do
    rules = [
      {&valid_version?/1, configuration},
      {&valid_name?/1, configuration},
      {&valid_metadata?/1, Map.get(configuration, :metadata)}
    ]

    eprules =
      configuration.endpoints
      |> Enum.map(fn ep ->
        {&valid_endpoint?/1, ep}
      end)

    results =
      (rules ++ eprules)
      |> Enum.map(fn {pred, input} ->
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
    bads =
      Enum.filter(md, fn {k, v} ->
        !is_binary(k) or !is_binary(v)
      end)
      |> length()

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

      e ->
        e
    end
  end
end
