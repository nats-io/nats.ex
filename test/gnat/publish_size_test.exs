defmodule Gnat.PublishSizeTest do
  use ExUnit.Case, async: true

  setup do
    conn = start_supervised!({Gnat, %{}})
    topic = "publish_size.#{System.unique_integer([:positive])}"
    %{conn: conn, topic: topic, limit: Gnat.server_info(conn).max_payload}
  end

  test "binary and nested iodata payloads accept the exact limit and reject one byte over", %{
    conn: conn,
    topic: topic,
    limit: limit
  } do
    {:ok, sid} = Gnat.sub(conn, self(), topic)

    for body <- [String.duplicate("x", limit), [<<0, 255>>, [String.duplicate("x", limit - 2)]]],
        opts <- [[], [reply_to: topic <> ".reply"]] do
      expected = IO.iodata_to_binary(body)
      assert :ok = Gnat.pub(conn, topic, body, opts)
      assert_receive {:msg, %{sid: ^sid, body: ^expected}}
      assert {:error, :max_payload_exceeded} = Gnat.pub(conn, topic, [body, 0], opts)
      assert :ok = Gnat.pub(conn, topic, "still usable")
      assert_receive {:msg, %{sid: ^sid, body: "still usable"}}
    end

    assert :ok = Gnat.pub(conn, topic, [])
    assert_receive {:msg, %{sid: ^sid, body: ""}}
    refute_received {:msg, _}
  end

  test "headers count their encoded bytes, version line and delimiters", %{
    conn: conn,
    topic: topic,
    limit: limit
  } do
    {:ok, sid} = Gnat.sub(conn, self(), topic)

    for headers <- [[], [{"x", ["é", [?a]]}, {"x", "second"}]],
        reply_opts <- [[], [reply_to: topic <> ".reply"]] do
      header_size = IO.iodata_length(Gnat.Headers.encode(headers)) + 12
      body = String.duplicate("x", limit - header_size)
      opts = [headers: headers] ++ reply_opts
      assert :ok = Gnat.pub(conn, topic, body, opts)
      assert_receive {:msg, %{sid: ^sid, body: ^body, headers: _}}
      assert {:error, :max_payload_exceeded} = Gnat.pub(conn, topic, [body, ?x], opts)
    end

    exact_headers = [{"x", String.duplicate("x", limit - 17)}]
    assert :ok = Gnat.pub(conn, topic, "", headers: exact_headers)
    assert_receive {:msg, %{sid: ^sid, body: "", headers: _}}

    assert {:error, :max_payload_exceeded} =
             Gnat.pub(conn, topic, "", headers: [{"x", String.duplicate("x", limit - 16)}])

    assert :ok = Gnat.pub(conn, topic, "after headers")
    assert_receive {:msg, %{sid: ^sid, body: "after headers"}}
    refute_received {:msg, _}
  end

  test "both request APIs reject oversized requests without registering a receiver", %{
    conn: conn,
    topic: topic,
    limit: limit
  } do
    {:ok, sid} = Gnat.sub(conn, self(), topic)

    for request <- [&Gnat.request/4, &Gnat.request_multi/4],
        opts <- [[], [headers: []], [headers: [{"x", ["é", [?a]]}]]] do
      header_size =
        case Keyword.fetch(opts, :headers) do
          :error -> 0
          {:ok, headers} -> IO.iodata_length(Gnat.Headers.encode(headers)) + 12
        end

      body = [String.duplicate("x", limit - header_size - 1), [0]]
      before = :sys.get_state(conn)

      assert {:error, :max_payload_exceeded} =
               request.(conn, topic, [body, ?x], opts ++ [receive_timeout: 100])

      assert :sys.get_state(conn).request_receivers == before.request_receivers
      assert :sys.get_state(conn).receivers == before.receivers

      task = Task.async(fn -> request.(conn, topic, body, opts ++ [max_messages: 1]) end)
      expected = IO.iodata_to_binary(body)
      assert_receive {:msg, %{sid: ^sid, body: ^expected, reply_to: reply}}
      assert :ok = Gnat.pub(conn, reply, "response")

      case Task.await(task) do
        {:ok, [%{body: "response"}]} -> :ok
        {:ok, %{body: "response"}} -> :ok
      end

      assert :sys.get_state(conn).request_receivers == before.request_receivers
      refute_received {:msg, _}
    end
  end

  test "rejected operations preserve an outstanding request", %{
    conn: conn,
    topic: topic,
    limit: limit
  } do
    {:ok, sid} = Gnat.sub(conn, self(), topic)
    task = Task.async(fn -> Gnat.request(conn, topic, "pending") end)
    assert_receive {:msg, %{sid: ^sid, body: "pending", reply_to: reply}}
    before = :sys.get_state(conn).request_receivers
    assert map_size(before) == 1

    for operation <- [&Gnat.pub/4, &Gnat.request/4, &Gnat.request_multi/4] do
      assert {:error, :max_payload_exceeded} =
               operation.(conn, topic, String.duplicate("x", limit + 1), [])

      assert :sys.get_state(conn).request_receivers == before
    end

    assert :ok = Gnat.pub(conn, reply, "response")
    assert {:ok, %{body: "response"}} = Task.await(task)
    assert :sys.get_state(conn).request_receivers == %{}
    refute_received {:msg, _}
  end

  test "a batch rejects only oversized members and applies the limit per message", %{
    conn: conn,
    topic: topic,
    limit: limit
  } do
    {:ok, sid} = Gnat.sub(conn, self(), topic)
    body = String.duplicate("x", limit)
    :ok = :sys.suspend(conn)

    requests =
      for {payload, result} <- [
            {[body, ?x], {:error, :max_payload_exceeded}},
            {body, :ok},
            {[body, ?x], {:error, :max_payload_exceeded}},
            {body, :ok},
            {[body, ?x], {:error, :max_payload_exceeded}}
          ] do
        {:gen_server.send_request(conn, {:pub, topic, payload, []}), result}
      end

    :ok = :sys.resume(conn)

    for {request, result} <- requests do
      assert {:reply, ^result} = :gen_server.wait_response(request, 1_000)
    end

    assert_receive {:msg, %{sid: ^sid, body: ^body}}
    assert_receive {:msg, %{sid: ^sid, body: ^body}}
    assert :ok = Gnat.pub(conn, topic, "after batch")
    assert_receive {:msg, %{sid: ^sid, body: "after batch"}}
    refute_received {:msg, _}
  end

  test "a rejected publish still flushes earlier buffered messages when the mailbox empties" do
    {conn, socket} = wire_connection(32)
    :ok = :sys.suspend(conn)

    {valid, oversized} =
      try do
        valid = :gen_server.send_request(conn, {:pub, "size", String.duplicate("x", 32), []})
        oversized = :gen_server.send_request(conn, {:pub, "size", String.duplicate("x", 33), []})
        {valid, oversized}
      after
        :ok = :sys.resume(conn)
      end

    assert {:reply, {:error, :max_payload_exceeded}} = :gen_server.wait_response(oversized, 1_000)
    assert {:reply, :ok} = :gen_server.wait_response(valid, 1_000)
    assert_wire_publish(socket, 32)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 20)
  end

  test "INFO updates change the limit used by subsequent publishes and requests" do
    {conn, socket} = wire_connection(32)
    assert :ok = Gnat.pub(conn, "size", String.duplicate("x", 32))
    assert_wire_publish(socket, 32)

    for limit <- [16, 64] do
      info = %{max_payload: limit, headers: true}
      :ok = :gen_tcp.send(socket, ["INFO ", Jason.encode!(info), "\r\nPING\r\n"])
      assert {:ok, "PONG\r\n"} = :gen_tcp.recv(socket, 0, 1_000)
      assert Gnat.server_info(conn).max_payload == limit

      for operation <- [&Gnat.pub/4, &Gnat.request/4, &Gnat.request_multi/4] do
        assert {:error, :max_payload_exceeded} =
                 operation.(conn, "size", String.duplicate("x", limit + 1), [])
      end

      assert :ok = Gnat.pub(conn, "size", String.duplicate("x", limit))
      assert_wire_publish(socket, limit)

      for request <- [&Gnat.request/4, &Gnat.request_multi/4] do
        task =
          Task.async(fn ->
            request.(conn, "size", String.duplicate("x", limit), max_messages: 1)
          end)

        assert {:ok, line} = :gen_tcp.recv(socket, 0, 1_000)
        assert ["PUB", "size", inbox, size] = String.split(line)
        assert String.to_integer(size) == limit
        assert {:ok, String.duplicate("x", limit) <> "\r\n"} == :gen_tcp.recv(socket, 0, 1_000)
        :ok = :gen_tcp.send(socket, ["MSG ", inbox, " 0 2\r\nok\r\n"])

        case Task.await(task) do
          {:ok, %{body: "ok"}} -> :ok
          {:ok, [%{body: "ok"}]} -> :ok
        end
      end
    end

    assert :sys.get_state(conn).request_receivers == %{}
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 20)
  end

  test "rejected header messages and requests write no bytes to the socket" do
    {conn, socket} = wire_connection(12)

    for operation <- [&Gnat.pub/4, &Gnat.request/4, &Gnat.request_multi/4] do
      assert {:error, :max_payload_exceeded} =
               operation.(conn, "size", "x", headers: [])
    end

    assert :ok = Gnat.pub(conn, "size", "", headers: [])
    assert {:ok, "HPUB size 12 12\r\n"} = :gen_tcp.recv(socket, 0, 1_000)
    assert {:ok, "NATS/1.0\r\n"} = :gen_tcp.recv(socket, 0, 1_000)
    assert {:ok, "\r\n"} = :gen_tcp.recv(socket, 0, 1_000)
    assert {:ok, "\r\n"} = :gen_tcp.recv(socket, 0, 1_000)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 20)
    assert :sys.get_state(conn).request_receivers == %{}
  end

  defp wire_connection(limit) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :line])
    {:ok, port} = :inet.port(listener)
    owner = self()

    accept =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)

        :ok =
          :gen_tcp.send(socket, [
            "INFO ",
            Jason.encode!(%{max_payload: limit, headers: true}),
            "\r\n"
          ])

        :ok = :gen_tcp.controlling_process(socket, owner)
        socket
      end)

    conn = start_supervised!(Supervisor.child_spec({Gnat, %{port: port}}, id: :wire_connection))
    socket = Task.await(accept)
    :ok = :gen_tcp.close(listener)
    on_exit(fn -> :gen_tcp.close(socket) end)
    assert {:ok, "CONNECT " <> _} = :gen_tcp.recv(socket, 0, 1_000)
    assert {:ok, "SUB " <> _} = :gen_tcp.recv(socket, 0, 1_000)
    {conn, socket}
  end

  defp assert_wire_publish(socket, size) do
    assert {:ok, "PUB size #{size}\r\n"} == :gen_tcp.recv(socket, 0, 1_000)
    assert {:ok, String.duplicate("x", size) <> "\r\n"} == :gen_tcp.recv(socket, 0, 1_000)
  end
end
