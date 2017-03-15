defmodule NeatoTest do
  use ExUnit.Case
  doctest Neato

  test "connect to a server" do
    {:ok, pid} = Neato.start_link()
    assert Process.alive?(pid)
    :ok = Neato.stop(pid)
  end

  test "subscribe to topic and receive a message" do
    {:ok, pid} = Neato.start_link()
    :ok = Neato.sub(pid, self(), "test")
    :ok = Neato.pub(pid, "test", "yo dawg")

    assert_receive {:msg, "test", "yo dawg"}, 1000
    :ok = Neato.stop(pid)
  end

  test "receive multiple messages" do
    {:ok, pid} = Neato.start_link()
    :ok = Neato.sub(pid, self(), "test")
    :ok = Neato.pub(pid, "test", "message 1")
    :ok = Neato.pub(pid, "test", "message 2")
    :ok = Neato.pub(pid, "test", "message 3")

    assert_receive {:msg, "test", "message 1"}, 1000
    assert_receive {:msg, "test", "message 2"}, 1000
    assert_receive {:msg, "test", "message 3"}, 1000
    :ok = Neato.stop(pid)
  end
end
