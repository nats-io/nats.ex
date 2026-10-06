defmodule Gnat.PublishOrderingTest do
  use ExUnit.Case, async: true

  setup do
    conn = start_supervised!({Gnat, %{ping_interval: 60_000}})
    topic = "ordering.#{System.unique_integer([:positive])}"
    %{conn: conn, topic: topic}
  end

  for count <- [1, 10, 11, 12, 22, 23] do
    test "#{count} queued publishes retain order across bounded batches", %{
      conn: conn,
      topic: topic
    } do
      {:ok, sid} = Gnat.sub(conn, self(), topic)
      socket = :sys.get_state(conn).socket
      {:ok, [send_cnt: before]} = :inet.getstat(socket, [:send_cnt])

      requests =
        queued(conn, fn ->
          for n <- 1..unquote(count) do
            opts = if rem(n, 2) == 0, do: [headers: Gnat.Headers.encode([{"x", "y"}])], else: []
            call_async(conn, {:pub, topic, Integer.to_string(n), opts})
          end
        end)

      Enum.each(requests, &assert_reply(&1, :ok))
      {:ok, [send_cnt: after_count]} = :inet.getstat(socket, [:send_cnt])
      assert after_count - before == div(unquote(count) + 10, 11)

      bodies =
        for _ <- 1..unquote(count) do
          assert_receive {:msg, %{sid: ^sid, body: body}}
          body
        end

      assert bodies == Enum.map(1..unquote(count), &Integer.to_string/1)
    end
  end

  test "queued publishes don't overtake an async subscription", %{conn: conn, topic: topic} do
    {before, subscription, after_sub} =
      queued(conn, fn ->
        before = call_async(conn, {:pub, topic, "before", []})
        subscription = Gnat.sub_async(conn, self(), topic)
        after_sub = call_async(conn, {:pub, topic, "after", []})
        {before, subscription, after_sub}
      end)

    assert_reply(before, :ok)
    assert {:reply, {:ok, sid}} = :gen_server.receive_response(elem(subscription, 0), 1_000)
    assert_reply(after_sub, :ok)
    assert_receive {:msg, %{sid: ^sid, body: body}}
    assert body == "after"
    refute_received {:msg, %{sid: ^sid}}
  end

  test "queued publishes don't overtake an async unsubscribe" do
    {conn, socket} = protocol_peer()
    {:ok, sid} = Gnat.sub(conn, self(), "topic")
    sub = "SUB topic #{sid}\r\n"
    assert {:ok, ^sub} = :gen_tcp.recv(socket, byte_size(sub), 1_000)

    {before, unsubscribe, after_unsub, stop} =
      queued(conn, fn ->
        before = call_async(conn, {:pub, "topic", "before", []})
        unsubscribe = Gnat.unsub_async(conn, sid)
        after_unsub = call_async(conn, {:pub, "topic", "after", []})
        stop = call_async(conn, :stop)
        {before, unsubscribe, after_unsub, stop}
      end)

    assert_reply(before, :ok)
    assert_reply(elem(unsubscribe, 0), :ok)
    assert_reply(after_unsub, :ok)
    assert_reply(stop, :ok)
    expected = "PUB topic 6\r\nbefore\r\nUNSUB #{sid}\r\nPUB topic 5\r\nafter\r\n"
    assert {:ok, ^expected} = :gen_tcp.recv(socket, byte_size(expected), 1_000)
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "queued publishes retain their position around a request", %{conn: conn, topic: topic} do
    {:ok, sid} = Gnat.sub(conn, self(), topic)

    {before, request, after_request} =
      queued(conn, fn ->
        before = call_async(conn, {:pub, topic, "before", []})

        request =
          call_async(conn, {:request, %{recipient: self(), topic: topic, body: "request"}})

        after_request = call_async(conn, {:pub, topic, "after", []})
        {before, request, after_request}
      end)

    assert_reply(before, :ok)
    assert {:reply, {:ok, inbox}} = :gen_server.receive_response(request, 1_000)
    assert_reply(after_request, :ok)

    bodies =
      for _ <- 1..3 do
        assert_receive {:msg, %{sid: ^sid, body: body}}
        body
      end

    assert bodies == ["before", "request", "after"]
    assert :ok = Gnat.pub(conn, inbox, "response")
    assert_receive {:msg, %{topic: ^inbox, body: "response"}}
    assert :ok = Gnat.unsub(conn, inbox)
  end

  for ping <- [:call, :info] do
    test "publish batches flush before a #{ping} ping and stop" do
      {conn, socket} = protocol_peer()

      {before, ping, after_ping, stop} =
        queued(conn, fn ->
          before = call_async(conn, {:pub, "topic", "before", []})

          ping =
            case unquote(ping) do
              :call ->
                call_async(conn, {:ping, self()})

              :info ->
                send(conn, :ping_check)
                nil
            end

          after_ping = call_async(conn, {:pub, "topic", "after", []})
          stop = call_async(conn, :stop)
          {before, ping, after_ping, stop}
        end)

      assert_reply(before, :ok)
      if ping, do: assert_reply(ping, :ok)
      assert_reply(after_ping, :ok)
      assert_reply(stop, :ok)
      # Gnat.ping/1 is a no-op; these paths exercise the internal protocol PING.
      expected = "PUB topic 6\r\nbefore\r\nPING\r\nPUB topic 5\r\nafter\r\n"
      assert {:ok, ^expected} = :gen_tcp.recv(socket, byte_size(expected), 1_000)
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
    end
  end

  test "concurrent public publishers preserve each sender's order", %{conn: conn, topic: topic} do
    {:ok, sid} = Gnat.sub(conn, self(), topic)

    publishers =
      for sender <- 1..4 do
        Task.async(fn ->
          for n <- 1..20 do
            :ok = Gnat.pub(conn, topic, "#{sender}:#{n}")
          end
        end)
      end

    Enum.each(publishers, &Task.await/1)

    deliveries =
      for _ <- 1..80 do
        assert_receive {:msg, %{sid: ^sid, body: body}}
        [sender, n] = String.split(body, ":")
        {String.to_integer(sender), String.to_integer(n)}
      end

    for sender <- 1..4 do
      assert for({^sender, n} <- deliveries, do: n) == Enum.to_list(1..20)
    end
  end

  test "public subscribe, publish, and request APIs preserve one sender's operation order", %{
    conn: conn,
    topic: topic
  } do
    parent = self()
    responder = start_supervised!({Task, fn -> respond_in_order(parent, topic) end})
    assert_receive {:ready, ^responder}
    {:ok, sid} = Gnat.sub(conn, self(), topic <> ".result")
    assert :ok = Gnat.pub(conn, topic, "before")
    assert {:ok, %{body: "before,request"}} = Gnat.request(conn, topic, "request")

    assert {:ok, [%{body: "before,request,multi"}]} =
             Gnat.request_multi(conn, topic, "multi", max_messages: 1)

    assert :ok = Gnat.pub(conn, topic <> ".result", "complete")
    assert_receive {:msg, %{sid: ^sid, body: "complete"}}
    assert :ok = Gnat.unsub(conn, sid)
  end

  defp respond_in_order(parent, topic) do
    {:ok, conn} = Gnat.start_link()
    {:ok, _} = Gnat.sub(conn, self(), topic)
    # A round trip on the responder connection makes its subscription visible before publishing.
    :ok = Gnat.pub(conn, topic, "ready")

    receive do
      {:msg, %{body: "ready"}} -> send(parent, {:ready, self()})
    end

    respond(conn, [])
  end

  defp respond(conn, bodies) do
    receive do
      {:msg, %{body: body, reply_to: reply}} ->
        bodies = bodies ++ [body]
        if reply, do: Gnat.pub(conn, reply, Enum.join(bodies, ","))
        respond(conn, bodies)
    end
  end

  # Queue the connection's call protocol from one sender while it is suspended. Public pub/4
  # waits for its reply, so sequential public calls alone can't create a publish backlog.
  defp call_async(conn, request), do: :gen_server.send_request(conn, request)

  defp assert_reply(request, expected) do
    assert {:reply, ^expected} = :gen_server.receive_response(request, 1_000)
  end

  defp queued(conn, fun) do
    :ok = :sys.suspend(conn)

    try do
      fun.()
    after
      :ok = :sys.resume(conn)
    end
  end

  defp protocol_peer do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :line])
    {:ok, port} = :inet.port(listener)
    parent = self()

    acceptor =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        :ok = :gen_tcp.send(socket, ~s(INFO {"max_payload":1048576}\r\n))
        :ok = :gen_tcp.controlling_process(socket, parent)
        socket
      end)

    conn =
      start_supervised!({Gnat, %{port: port, ping_interval: 60_000}},
        id: :protocol_peer,
        restart: :temporary
      )

    socket = Task.await(acceptor)
    :ok = :gen_tcp.close(listener)
    assert {:ok, "CONNECT " <> _} = :gen_tcp.recv(socket, 0, 1_000)
    assert {:ok, "SUB " <> _} = :gen_tcp.recv(socket, 0, 1_000)
    :ok = :inet.setopts(socket, packet: :raw)
    on_exit(fn -> :gen_tcp.close(socket) end)
    {conn, socket}
  end
end
