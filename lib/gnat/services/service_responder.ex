defmodule Gnat.Services.ServiceResponder do
  @moduledoc false

  require Logger
  alias Gnat.Services.Service

  @op_ping "PING"
  @op_stats "STATS"
  @op_info "INFO"

  def maybe_respond(%{topic: topic} = message, service) do
    case String.split(topic, ".") do
      ["$SRV", @op_ping | rest] ->
        handle_ping(rest, service, message)

      ["$SRV", @op_info | rest] ->
        handle_info(rest, service, message)

      ["$SRV", @op_stats | rest] ->
        handle_stats(rest, service, message)

      _other ->
        Logger.error("ServiceResponder received unexpected message #{topic}")
    end
  end

  defp handle_ping(tail, service, %{reply_to: rt, gnat: gnat}) do
    if should_respond?(tail, service.name, service.instance_id) do
      body = Service.ping(service) |> Jason.encode!()
      Gnat.pub(gnat, rt, body)
    end
  end

  defp handle_info(tail, service, %{reply_to: rt, gnat: gnat}) do
    if should_respond?(tail, service.name, service.instance_id) do
      body = Service.info(service) |> Jason.encode!()
      Gnat.pub(gnat, rt, body)
    end
  end

  defp handle_stats(tail, service, %{reply_to: rt, gnat: gnat}) do
    if should_respond?(tail, service.name, service.instance_id) do
      body = Service.stats(service) |> Jason.encode!()
      Gnat.pub(gnat, rt, body)
    end
  end

  @spec should_respond?(list, String.t(), String.t()) :: boolean()
  defp should_respond?(tail, service_name, instance_id) do
    case tail do
      [] -> true
      [^service_name] -> true
      [^service_name, ^instance_id] -> true
      _ -> false
    end
  end
end
