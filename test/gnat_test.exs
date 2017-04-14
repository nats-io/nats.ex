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

  @tag :multi_server
  test "connect to a server with authentication" do
    connection_settings = %{
      host: 'localhost',
      port: 4223,
      tcp_opts: [:binary],
      username: "bob",
      password: "alice"
    }
    {:ok, pid} = Gnat.start_link(connection_settings)
    assert Process.alive?(pid)
    :ok = Gnat.ping(pid)
    :ok = Gnat.stop(pid)
  end

  @tag :multi_server
  test "connet to a server which requires TLS" do
    connection_settings = %{port: 4224, tls: true}
    {:ok, gnat} = Gnat.start_link(connection_settings)
    assert Gnat.ping(gnat) == :ok
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
    assert Gnat.ping(gnat) == :ok
    assert Gnat.stop(gnat) == :ok
  end

  test "subscribe to topic and receive a message" do
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "yo dawg")

    assert_receive {:msg, %{topic: "test", body: "yo dawg", reply_to: nil}}, 1000
    :ok = Gnat.stop(pid)
  end

  test "subscribe receive a message with a reply_to" do
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "with_reply")
    :ok = Gnat.pub(pid, "with_reply", "yo dawg", reply_to: "me")

    assert_receive {:msg, %{topic: "with_reply", reply_to: "me", body: "yo dawg"}}, 1000
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

  test "request-reply convenience function" do
    topic = "req-resp"
    {:ok, pid} = Gnat.start_link()
    spin_up_echo_server_on_topic(pid, topic)
    {:ok, msg} = Gnat.request(pid, topic, "ohai", receive_timeout: 500)
    assert msg.body == "ohai"
  end

  defp spin_up_echo_server_on_topic(gnat, topic) do
    spawn(fn ->
      {:ok, subscription} = Gnat.sub(gnat, self(), topic)
      :ok = Gnat.unsub(gnat, subscription, max_messages: 1)
      receive do
        {:msg, %{topic: ^topic, body: body, reply_to: reply_to}} ->
          Gnat.pub(gnat, reply_to, body)
      end
    end)
  end
end
