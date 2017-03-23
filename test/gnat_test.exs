defmodule GnatTest do
  use ExUnit.Case
  doctest Gnat

  test "connect to a server" do
    {:ok, pid} = Gnat.start_link()
    assert Process.alive?(pid)
    :ok = Gnat.stop(pid)
  end

  test "subscribe to topic and receive a message" do
    {:ok, pid} = Gnat.start_link()
    :ok = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "yo dawg")

    assert_receive {:msg, "test", "yo dawg"}, 1000
    :ok = Gnat.stop(pid)
  end

  test "receive multiple messages" do
    {:ok, pid} = Gnat.start_link()
    :ok = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "message 1")
    :ok = Gnat.pub(pid, "test", "message 2")
    :ok = Gnat.pub(pid, "test", "message 3")

    assert_receive {:msg, "test", "message 1"}, 1000
    assert_receive {:msg, "test", "message 2"}, 1000
    assert_receive {:msg, "test", "message 3"}, 1000
    :ok = Gnat.stop(pid)
  end
end
