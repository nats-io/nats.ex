defmodule Gnat.ProtocolValidationTest do
  use ExUnit.Case, async: true

  @invalid_subjects [
    "",
    "foo bar",
    "foo\tbar",
    "foo\rbar",
    "foo\nbar",
    "foo\r\nPING\r\n",
    nil,
    :topic,
    ~c"topic",
    ["topic"]
  ]
  @non_delimiter_bytes for byte <- 0..255, byte not in [9, 10, 13, 32], do: <<"a", byte, "b">>
  @server_validated_subjects [
                               ".",
                               ".foo",
                               "foo.",
                               "foo..bar",
                               "*",
                               ">",
                               "foo.*",
                               "foo.>",
                               "foo*bar",
                               "foo>bar",
                               ">.foo",
                               "foo.>.bar",
                               "**",
                               "foo\u00A0bar",
                               "foo\u2000bar",
                               "foo\u2028bar",
                               "foo\u3000bar"
                             ] ++ @non_delimiter_bytes

  test "publish rejects invalid subjects before contacting the connection" do
    for topic <- @invalid_subjects, opts <- [[], [headers: [{"x", "y"}]]] do
      assert_raise ArgumentError, ~r/invalid publish subject/, fn ->
        Gnat.pub(self(), topic, "payload", opts)
      end
    end
  end

  test "publish rejects invalid reply subjects with and without headers" do
    for reply <- @invalid_subjects, opts <- [[], [headers: [{"x", "y"}]]] do
      assert_raise ArgumentError, ~r/invalid reply subject/, fn ->
        Gnat.pub(self(), "topic", "payload", Keyword.put(opts, :reply_to, reply))
      end
    end
  end

  test "both request APIs reject invalid subjects before registering a request" do
    for topic <- @invalid_subjects,
        request <- [&Gnat.request/4, &Gnat.request_multi/4],
        opts <- [[], [headers: [{"x", "y"}]]] do
      assert_raise ArgumentError, ~r/invalid publish subject/, fn ->
        request.(self(), topic, "payload", opts)
      end
    end
  end

  test "subscribe rejects protocol delimiters before contacting the connection" do
    for topic <- @invalid_subjects, subscribe <- [&Gnat.sub/3, &Gnat.sub_async/3] do
      assert_raise ArgumentError, ~r/invalid subscription subject/, fn ->
        subscribe.(self(), self(), topic)
      end
    end

    refute_received {:"$gen_call", _, _}
  end

  test "subscribe rejects non-binary queue groups and protocol delimiters" do
    for queue <-
          [nil, :queue, ~c"queue", ["queue"]] ++
            Enum.map([9, 10, 13, 32], &"workers#{<<&1::utf8>>}east") do
      assert_raise ArgumentError, ~r/invalid queue group/, fn ->
        Gnat.sub(self(), self(), "topic", queue_group: queue)
      end
    end
  end

  test "invalid inbox prefixes fail before attempting a connection" do
    for prefix <- [
          nil,
          :inbox,
          ~c"inbox",
          "a\t",
          "a\r",
          "a\n",
          "a\r\nPING\r\n",
          "a "
        ] do
      assert_raise ArgumentError, ~r/invalid inbox prefix/, fn ->
        Gnat.start_link(%{host: "127.0.0.1", port: 0, inbox_prefix: prefix})
      end
    end
  end

  test "publish leaves subject grammar and non-delimiter bytes to the server" do
    for topic <- @server_validated_subjects do
      assert_connection_call(
        fn conn -> Gnat.pub(conn, topic, "payload", reply_to: topic) end,
        {:pub, topic, "payload", [reply_to: topic]},
        :ok
      )
    end
  end

  test "subscribe leaves subject and queue grammar and non-delimiter bytes to the server" do
    subscriber = self()

    for topic <- @server_validated_subjects do
      assert_connection_call(
        fn conn -> Gnat.sub(conn, subscriber, topic, queue_group: topic) end,
        {:sub, subscriber, topic, [queue_group: topic]},
        {:ok, 1}
      )
    end
  end

  test "inbox prefixes allow grammar and non-delimiter bytes for the server to validate" do
    for prefix <- ["" | @server_validated_subjects] do
      assert :ok = Gnat.Validation.inbox_prefix!(prefix)
    end
  end

  test "empty queue groups behave as unqueued subscriptions" do
    {:ok, conn} = Gnat.start_link()
    topic = "validation.#{System.unique_integer([:positive])}.empty_queue"
    {:ok, first} = Gnat.sub(conn, self(), topic, queue_group: "")
    {:ok, second} = Gnat.sub(conn, self(), topic, queue_group: "")
    assert :ok = Gnat.pub(conn, topic, "data")
    assert_receive {:msg, %{sid: ^first, body: "data"}}
    assert_receive {:msg, %{sid: ^second, body: "data"}}
    assert :ok = Gnat.stop(conn)
  end

  test "invalid operations leave connection state unchanged and valid traffic still works" do
    {:ok, conn} = Gnat.start_link()
    before = :sys.get_state(conn)

    for operation <- [
          fn -> Gnat.pub(conn, "bad\r\nPING\r\n", "data") end,
          fn -> Gnat.pub(conn, "good", "data", reply_to: "bad\r\nPING\r\n") end,
          fn -> Gnat.sub(conn, self(), "bad\r\nPING\r\n") end,
          fn -> Gnat.sub_async(conn, self(), "bad\r\nPING\r\n") end,
          fn -> Gnat.sub(conn, self(), "good", queue_group: "bad\r\nPING\r\n") end,
          fn -> Gnat.request(conn, "bad\r\nPING\r\n", "data") end,
          fn -> Gnat.request_multi(conn, "bad\r\nPING\r\n", "data") end
        ] do
      assert_raise ArgumentError, operation
    end

    assert :sys.get_state(conn) == before
    topic = "validation.#{System.unique_integer([:positive])}"
    {:ok, sid} = Gnat.sub(conn, self(), topic)
    assert :ok = Gnat.pub(conn, topic, ["binary", <<0, 13, 10, 255>>])
    assert_receive {:msg, %{sid: ^sid, body: <<"binary", 0, 13, 10, 255>>}}
    assert :ok = Gnat.stop(conn)
  end

  test "async wildcard subscriptions receive replies and deliveries" do
    {:ok, conn} = Gnat.start_link()
    prefix = "validation.#{System.unique_integer([:positive])}"
    request = Gnat.sub_async(conn, self(), prefix <> ".*")
    assert_receive response
    assert {:reply, {:ok, sid}} = Gnat.subscription_response(response, request)

    assert :ok = Gnat.pub(conn, prefix <> ".orders", "data")
    assert_receive {:msg, %{sid: ^sid, body: "data"}}
    assert :ok = Gnat.stop(conn)
  end

  test "literal subjects, replies, wildcard subscriptions and queue groups round trip" do
    {:ok, conn} = Gnat.start_link()
    prefix = "validation.#{System.unique_integer([:positive])}"
    topic = prefix <> ".orders/region:ok-_=$"
    reply = prefix <> ".reply"
    {:ok, exact} = Gnat.sub(conn, self(), topic, queue_group: "workers.eu-1_東京")
    {:ok, single} = Gnat.sub(conn, self(), prefix <> ".*")
    {:ok, tail} = Gnat.sub(conn, self(), prefix <> ".>")
    {:ok, mixed} = Gnat.sub(conn, self(), "validation.*.>")

    for opts <- [[], [headers: [{"x", "y"}]]] do
      assert :ok = Gnat.pub(conn, topic, "data", Keyword.put(opts, :reply_to, reply))

      for sid <- [exact, single, tail, mixed] do
        assert_receive {:msg, %{sid: ^sid, topic: ^topic, reply_to: ^reply, body: "data"}}
      end
    end

    assert :ok = Gnat.stop(conn)
  end

  test "UTF-8 subjects and replies are sent unchanged" do
    {:ok, conn} = Gnat.start_link()
    port = Gnat.server_info(conn).port
    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false, packet: :line])
    {:ok, "INFO " <> _} = :gen_tcp.recv(socket, 0, 1_000)
    topic = "validation.#{System.unique_integer([:positive])}.東京"
    reply = "réponse.東京"
    :ok = :gen_tcp.send(socket, [~s(CONNECT {"verbose":false}\r\nSUB ), topic, " 1\r\nPING\r\n"])
    assert {:ok, "PONG\r\n"} = :gen_tcp.recv(socket, 0, 1_000)
    assert :ok = Gnat.pub(conn, topic, "data", reply_to: reply)
    assert {:ok, "MSG #{topic} 1 #{reply} 4\r\n"} == :gen_tcp.recv(socket, 0, 1_000)
    assert {:ok, "data\r\n"} = :gen_tcp.recv(socket, 0, 1_000)
    :ok = :gen_tcp.close(socket)
    assert :ok = Gnat.stop(conn)
  end

  test "custom inbox prefixes support both request APIs" do
    for prefix <- ["", "custom", "custom._INBOX."] do
      {:ok, conn} = Gnat.start_link(%{inbox_prefix: prefix})
      assert {:ok, %{body: "data"}} = Gnat.request(conn, "rpc.validation", "data")

      assert {:ok, [%{body: "data"}]} =
               Gnat.request_multi(conn, "rpc.validation", "data", max_messages: 1)

      assert :ok = Gnat.stop(conn)
    end
  end

  defp assert_connection_call(operation, expected, reply) do
    conn = self()
    task = Task.async(fn -> operation.(conn) end)
    assert_receive {:"$gen_call", from, ^expected}
    GenServer.reply(from, reply)
    assert Task.await(task) == reply
  end
end
