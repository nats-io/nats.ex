defmodule GnatTest do
  use ExUnit.Case, async: true
  doctest Gnat

  setup context do
    CheckForExpectedNatsServers.check(Map.keys(context))
    :ok
  end

  test "connect to a server" do
    {:ok, pid} = Gnat.start_link()
    assert Process.alive?(pid)
    :ok = Gnat.stop(pid)
  end

  # We have to skip this test in CI builds because CircleCI doesn't enable IPv6 in it's docker
  # configuration. See https://circleci.com/docs/faq#can-i-use-ipv6-in-my-tests
  @tag :ci_skip
  test "connect to a server over IPv6" do
    {:ok, pid} = Gnat.start_link(%{host: '::1', tcp_opts: [:binary, :inet6]})
    assert Process.alive?(pid)
    :ok = Gnat.stop(pid)
  end

  @tag :multi_server
  test "connect to a server with user/pass authentication" do
    connection_settings = %{
      host: "localhost",
      port: 4223,
      tcp_opts: [:binary],
      username: "bob",
      password: "alice"
    }
    {:ok, pid} = Gnat.start_link(connection_settings)
    assert Process.alive?(pid)
    :ok = Gnat.stop(pid)
  end

  @tag :multi_server
  test "connect to a server with token authentication" do
    connection_settings = %{
      host: "localhost",
      port: 4226,
      tcp_opts: [:binary],
      token: "SpecialToken",
      auth_required: true
    }
    {:ok, pid} = Gnat.start_link(connection_settings)
    assert Process.alive?(pid)
    :ok = Gnat.stop(pid)
  end

  @tag :multi_server
  test "connet to a server which requires TLS" do
    connection_settings = %{port: 4224, tls: true}
    {:ok, gnat} = Gnat.start_link(connection_settings)
    assert Gnat.stop(gnat) == :ok
  end

  @tag :multi_server
  test "connect to a server which requires TLS with a client certificate" do
    connection_settings = %{
      port: 4225,
      tls: true,
      ssl_opts: [
        certfile: "test/fixtures/client-cert.pem",
        keyfile: "test/fixtures/client-key.pem",
      ],
    }
    {:ok, gnat} = Gnat.start_link(connection_settings)
    assert Gnat.stop(gnat) == :ok
  end

  @tag :multi_server
  test "connect to a server which requires nkeys" do
    connection_settings = %{
      port: 4227,
      nkey_seed: File.read!("test/fixtures/nkey_seed")
    }
    {:ok, gnat} = Gnat.start_link(connection_settings)
    assert Gnat.stop(gnat) == :ok
  end

  test "subscribe to topic and receive a message" do
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "yo dawg")

    assert_receive {:msg, %{topic: "test", body: "yo dawg", reply_to: nil}}, 1000
    :ok = Gnat.stop(pid)
  end

  test "subscribe to topic and receive a message with headers" do
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "sub_with_headers")
    headers = [{"X", "foo"}]
    :ok = Gnat.pub(pid, "sub_with_headers", "yo dawg", headers: headers)

    assert_receive {:msg, %{topic: "sub_with_headers", body: "yo dawg", reply_to: nil, headers: [{"x", "foo"}]}}, 1000
    :ok = Gnat.stop(pid)
  end

  test "subscribe receive a message with a reply_to" do
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "with_reply")
    :ok = Gnat.pub(pid, "with_reply", "yo dawg", reply_to: "me")

    assert_receive {:msg, %{topic: "with_reply", reply_to: "me", body: "yo dawg"}}, 1000
    :ok = Gnat.stop(pid)
  end

  test "subscribe receive a message with a reply_to and headers" do
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "with_reply")
    headers = [{"x", "y"}]
    :ok = Gnat.pub(pid, "with_reply", "yo dawg", reply_to: "me", headers: headers)

    assert_receive {:msg, %{topic: "with_reply", reply_to: "me", body: "yo dawg", headers: ^headers}}, 1000
    :ok = Gnat.stop(pid)
  end

  test "receive multiple messages" do
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "message 1")
    :ok = Gnat.pub(pid, "test", "message 2")
    :ok = Gnat.pub(pid, "test", "message 3")

    assert_receive {:msg, %{topic: "test", body: "message 1", reply_to: nil}}, 500
    assert_receive {:msg, %{topic: "test", body: "message 2", reply_to: nil}}, 500
    assert_receive {:msg, %{topic: "test", body: "message 3", reply_to: nil}}, 500
    :ok = Gnat.stop(pid)
  end

  test "subscribing to the same topic multiple times" do
    {:ok, pid} = Gnat.start_link()
    {:ok, _sub1} = Gnat.sub(pid, self(), "dup")
    {:ok, _sub2} = Gnat.sub(pid, self(), "dup")
    :ok = Gnat.pub(pid, "dup", "yo")
    :ok = Gnat.pub(pid, "dup", "ma")
    assert_receive {:msg, %{topic: "dup", body: "yo"}}, 500
    assert_receive {:msg, %{topic: "dup", body: "yo"}}, 500
    assert_receive {:msg, %{topic: "dup", body: "ma"}}, 500
    assert_receive {:msg, %{topic: "dup", body: "ma"}}, 500
  end

  test "subscribing to the same topic multiple times with a queue group" do
    {:ok, pid} = Gnat.start_link()
    {:ok, _sub1} = Gnat.sub(pid, self(), "dup", queue_group: "us")
    {:ok, _sub2} = Gnat.sub(pid, self(), "dup", queue_group: "us")
    :ok = Gnat.pub(pid, "dup", "yo")
    :ok = Gnat.pub(pid, "dup", "ma")
    assert_receive {:msg, %{topic: "dup", body: "yo"}}, 500
    assert_receive {:msg, %{topic: "dup", body: "ma"}}, 500
    receive do
      {:msg, %{topic: _topic}}=msg -> flunk("Received duplicate message: #{inspect msg}")
      after 200 -> :ok
    end
  end

  test "unsubscribing from a topic" do
    topic = "testunsub"
    {:ok, pid} = Gnat.start_link()
    {:ok, sub_ref} = Gnat.sub(pid, self(), topic)
    :ok = Gnat.pub(pid, topic, "msg1")
    assert_receive {:msg, %{topic: ^topic, body: "msg1"}}, 500
    :ok = Gnat.unsub(pid, sub_ref)
    :ok = Gnat.pub(pid, topic, "msg2")
    receive do
      {:msg, %{topic: _topic, body: _body}}=msg -> flunk("Received message after unsubscribe: #{inspect msg}")
      after 200 -> :ok
    end
  end

  test "unsubscribing from a topic after a maximum number of messages" do
    topic = "testunsub_maxmsg"
    {:ok, pid} = Gnat.start_link()
    {:ok, sub_ref} = Gnat.sub(pid, self(), topic)
    :ok = Gnat.unsub(pid, sub_ref, max_messages: 2)
    :ok = Gnat.pub(pid, topic, "msg1")
    :ok = Gnat.pub(pid, topic, "msg2")
    :ok = Gnat.pub(pid, topic, "msg3")
    assert_receive {:msg, %{topic: ^topic, body: "msg1"}}, 500
    assert_receive {:msg, %{topic: ^topic, body: "msg2"}}, 500
    receive do
      {:msg, _topic, _msg}=msg -> flunk("Received message after unsubscribe: #{inspect msg}")
      after 200 -> :ok
    end
  end

  test "request-reply convenience function with headers" do
    topic = "req-resp"
    {:ok, pid} = Gnat.start_link()
    spin_up_echo_server_on_topic(self(), pid, topic)
    # Wait for server to spawn and subscribe.
    assert_receive(true, 100)
    {:ok, msg} = Gnat.request(pid, topic, "ohai", receive_timeout: 500)
    assert msg.body == "ohai"
  end

  test "request-reply convenience function" do
    topic = "req-resp"
    {:ok, pid} = Gnat.start_link()
    spin_up_echo_server_on_topic(self(), pid, topic)
    # Wait for server to spawn and subscribe.
    assert_receive(true, 100)
    headers = [{"accept", "json"}]
    {:ok, msg} = Gnat.request(pid, topic, "ohai", receive_timeout: 500, headers: headers)
    assert msg.body == "ohai"
    assert msg.headers == headers
  end

  test "request_multi convenience function with no maximum messages" do
    topic = "req.multi"
    {:ok, pid} = Gnat.start_link()
    # start 4 servers to get 4 responses
    Enum.each(1..4, fn _i ->
      spin_up_echo_server_on_topic(self(), pid, topic)
      assert_receive(true, 100)
    end)

    {:ok, messages} = Gnat.request_multi(pid, topic, "ohai", receive_timeout: 500)
    assert Enum.count(messages) == 4
    assert Enum.all?(messages, fn msg -> msg.body == "ohai" end)
  end

  test "request_multi convenience function with maximum messages" do
    topic = "req.multi2"
    {:ok, pid} = Gnat.start_link()
    # start 4 servers to get 4 responses
    Enum.each(1..4, fn _i ->
      spin_up_echo_server_on_topic(self(), pid, topic)
      assert_receive(true, 100)
    end)

    {:ok, messages} = Gnat.request_multi(pid, topic, "ohai", max_messages: 4, receive_timeout: 500)
    assert Enum.count(messages) == 4
    assert Enum.all?(messages, fn msg -> msg.body == "ohai" end)
  end

  test "request_multi convenience function with maximum messages not met" do
    topic = "req.multi2"
    {:ok, pid} = Gnat.start_link()
    # start 4 servers to get 4 responses
    Enum.each(1..4, fn _i ->
      spin_up_echo_server_on_topic(self(), pid, topic)
      assert_receive(true, 100)
    end)

    {:ok, messages} = Gnat.request_multi(pid, topic, "ohai", max_messages: 8, receive_timeout: 500)
    assert Enum.count(messages) == 4
    assert Enum.all?(messages, fn msg -> msg.body == "ohai" end)
  end

  defp spin_up_echo_server_on_topic(ready, gnat, topic) do
    spawn(fn ->
      {:ok, subscription} = Gnat.sub(gnat, self(), topic)
      :ok = Gnat.unsub(gnat, subscription, max_messages: 1)
      send ready, true
      receive do
        {:msg, %{topic: ^topic, body: body, reply_to: reply_to, headers: headers}} ->
          Gnat.pub(gnat, reply_to, body, headers: headers)

        {:msg, %{topic: ^topic, body: body, reply_to: reply_to}} ->
          Gnat.pub(gnat, reply_to, body)
      end
    end)
  end

  test "recording errors from the broker" do
    import ExUnit.CaptureLog
    {:ok, gnat} = Gnat.start_link()
    assert capture_log(fn ->
      Process.flag(:trap_exit, true)
      Gnat.sub(gnat, self(), "invalid. subject")
      Process.sleep(20) # errors are reported asynchronously so we need to wait a moment
    end) =~ "Invalid Subject"
  end

  test "connection timeout" do
    start = System.monotonic_time(:millisecond)
    connection_settings = %{ host: '169.33.33.33', connection_timeout: 200 }
    {:stop, :timeout} = Gnat.init(connection_settings)
    assert_in_delta System.monotonic_time(:millisecond) - start, 200, 10
  end

  test "request-reply with custom inbox prefix" do
    topic = "req-resp"
    {:ok, pid} = Gnat.start_link(%{inbox_prefix: "custom._INBOX."})
    spin_up_echo_server_on_topic(self(), pid, topic)
    # Wait for server to spawn and subscribe.
    assert_receive(true, 100)
    headers = [{"accept", "json"}]
    {:ok, msg} = Gnat.request(pid, topic, "ohai", receive_timeout: 500, headers: headers)
    assert "custom._INBOX." <> _ = msg.topic
  end

  test "server_info/1 returns server info" do
    {:ok, pid} = Gnat.start_link()
    info = Gnat.server_info(pid)
    assert Map.has_key?(info, :version)
    assert is_binary(info.version)
  end
end
