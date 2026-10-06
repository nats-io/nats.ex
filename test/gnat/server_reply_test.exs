defmodule Gnat.ServerReplyTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  alias Gnat.Services.Service

  defmodule Server do
    use Gnat.Server
    def request(message), do: Gnat.ServerReplyTest.reply(message)
    def error(message, error), do: Gnat.ServerReplyTest.handle_error(message, error)
  end

  defmodule ServiceServer do
    use Gnat.Services.Server
    def request(message, _endpoint, _group), do: Gnat.ServerReplyTest.reply(message)
    def error(message, error), do: Gnat.ServerReplyTest.handle_error(message, error)
  end

  defmodule DefaultServer do
    use Gnat.Server
    def request(message), do: Gnat.ServerReplyTest.reply(message)
  end

  defmodule DefaultServiceServer do
    use Gnat.Services.Server
    def request(message, _endpoint, _group), do: Gnat.ServerReplyTest.reply(message)
  end

  def reply(%{body: "valid"}), do: {:reply, "valid response"}
  def reply(%{body: "application_error"}), do: {:error, :application_error}
  def reply(message), do: {:reply, oversized_body(message)}

  def handle_error(message, error) do
    send(message.test_pid, {:handled_error, error})

    if message.body in ["oversized_error_reply", "application_error"] do
      {:reply, oversized_body(message)}
    else
      {:reply, "smaller error response"}
    end
  end

  defp oversized_body(message) do
    [String.duplicate("x", Gnat.server_info(message.gnat).max_payload), [?x]]
  end

  setup do
    conn = start_supervised!({Gnat, %{}})
    topic = "server_reply.#{System.unique_integer([:positive])}"
    {:ok, _sid} = Gnat.sub(conn, self(), topic)

    {:ok, service} =
      Service.init(%{
        name: "server_reply",
        description: "reply tests",
        version: "1.0.0",
        endpoints: [%{name: "reply", subject: topic}]
      })

    handler_id = make_ref()

    :ok =
      :telemetry.attach_many(
        handler_id,
        [[:gnat, :service_request], [:gnat, :service_error]],
        &__MODULE__.handle_telemetry/4,
        {self(), topic}
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    %{conn: conn, topic: topic, service: service}
  end

  def handle_telemetry(event, measurements, %{topic: topic}, {pid, topic}) do
    send(pid, {:service_event, event, measurements})
  end

  def handle_telemetry(_event, _measurements, _metadata, _config), do: :ok

  for {kind, responder, default_responder} <- [
        {:server, Server, DefaultServer},
        {:service, ServiceServer, DefaultServiceServer}
      ] do
    test "#{kind} invokes the error callback when a reply exceeds max_payload", context do
      task = request(context, "oversized")
      execute(unquote(kind), unquote(responder), context)
      assert_receive {:handled_error, :max_payload_exceeded}
      refute_received {:handled_error, _}
      assert {:ok, %{body: "smaller error response"}} = Task.await(task)
      assert_error_accounting(unquote(kind), context)
      assert_valid_reply(unquote(kind), unquote(responder), context)
    end

    test "#{kind} logs an oversized error response without calling the handler again", context do
      for body <- ["oversized_error_reply", "application_error"] do
        task = request(context, body)
        log = capture_log(fn -> execute(unquote(kind), unquote(responder), context) end)

        expected_error =
          if body == "application_error", do: :application_error, else: :max_payload_exceeded

        assert_receive {:handled_error, ^expected_error}
        refute_received {:handled_error, _}
        assert log =~ "could not send reply"
        assert log =~ "max_payload_exceeded"
        assert {:error, :timeout} = Task.await(task)
      end

      assert_error_accounting(unquote(kind), context, 2)
      assert_valid_reply(unquote(kind), unquote(responder), context, 2)
    end

    test "#{kind} default error callback logs a rejected reply", context do
      task = request(context, "oversized")
      log = capture_log(fn -> execute(unquote(kind), unquote(default_responder), context) end)
      assert log =~ "max_payload_exceeded"
      assert {:error, :timeout} = Task.await(task)
      assert_error_accounting(unquote(kind), context)
      assert_valid_reply(unquote(kind), unquote(default_responder), context)
    end
  end

  defp request(%{conn: conn, topic: topic}, body) do
    Task.async(fn -> Gnat.request(conn, topic, body, receive_timeout: 1_000) end)
  end

  defp execute(kind, responder, %{topic: topic, service: service}) do
    assert_receive {:msg, %{topic: ^topic} = message}
    message = Map.put(message, :test_pid, self())

    case kind do
      :server -> Gnat.Server.execute(responder, message)
      :service -> Gnat.Services.Server.execute(responder, message, service)
    end
  end

  defp assert_error_accounting(kind, context, errors \\ 1)

  defp assert_error_accounting(:server, _context, _errors) do
    refute_received {:service_event, _, _}
  end

  defp assert_error_accounting(:service, %{service: service}, errors) do
    assert %{endpoints: [%{num_requests: 0, num_errors: ^errors}]} = Service.stats(service)

    for _ <- 1..errors do
      assert_receive {:service_event, [:gnat, :service_error], %{latency: latency}}
      assert is_integer(latency) and latency >= 0
    end

    refute_received {:service_event, _, _}
  end

  defp assert_valid_reply(kind, responder, context, errors \\ 1) do
    assert Process.alive?(context.conn)
    task = request(context, "valid")
    execute(kind, responder, context)
    assert {:ok, %{body: "valid response"}} = Task.await(task)

    if kind == :service do
      assert %{endpoints: [%{num_requests: 1, num_errors: ^errors}]} =
               Service.stats(context.service)

      assert_receive {:service_event, [:gnat, :service_request], %{latency: _}}
    end

    refute_received {:handled_error, _}
    refute_received {:service_event, _, _}
  end
end
