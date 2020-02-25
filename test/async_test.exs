defmodule Gnat.AsyncTest do
  use ExUnit.Case, async: true
  alias Gnat.Async

  @tag capture_log: true
  test "messages are eventually published" do
    {:ok, _pid} = Gnat.start_link(%{}, name: :async_pub_c1)
    {:ok, _pid} = Async.start_link(%{connection_name: :async_pub_c1, name: :async_pub_q1})
    {:ok, _ref} = Gnat.sub(:async_pub_c1, self(), "async_pub_t1")
    assert :ok = Async.pub(:async_pub_q1, "async_pub_t1", "Ahoy there, Matey")
    assert_receive {:msg, %{topic: "async_pub_t1", body: "Ahoy there, Matey"}}, 1000
  end

  @tag capture_log: true
  test "messages can be published without the connection being available" do
    # start anonymous connection and subscribe to topic
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "async_pub_t2")

    # start async pub and send it a message before its connection is available
    {:ok, _pid} = Async.start_link(%{connection_name: :async_pub_c2, name: :async_pub_q2})
    assert :ok = Async.pub(:async_pub_q2, "async_pub_t2", "Ron Swanson", reply_to: "me")

    # start the connection that our async pub wants to use
    {:ok, _pid} = Gnat.start_link(%{}, name: :async_pub_c2)

    # it should have used the new connection to publish our message and then we get it from our
    # anonymous connection
    assert_receive {:msg, %{topic: "async_pub_t2", body: "Ron Swanson", reply_to: "me"}}, 1000
  end

  @tag capture_log: true
  test "a graceful shutdown is attempted when being shutdown by a supervisor" do
    # start anonymous connection and subscribe to topic
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "async_pub_t3")

    # start async pub and send it a message before its connection is available
    {:ok, async} = Async.start_link(%{connection_name: :async_pub_c3, name: :async_pub_q3})
    assert :ok = Async.pub(:async_pub_q3, "async_pub_t3", "Ron Swanson", reply_to: "me")

    # initiate a graceful shutdown - this must be done in another process because Supervisor.stop is synchronous
    test_pid = self()
    spawn_link(fn ->
      stopped = Supervisor.stop(async, :normal)
      Process.send(test_pid, {:shutdown, stopped}, [])
    end)
    :timer.sleep(5) # give a small amount of time for the shutdown signal to be received
    assert {:error, :shutting_down} = Async.pub(:async_pub_q3, "async_pub_t3", "Ron Swanson", reply_to: "me")

    # start the connection that our async pub wants to use
    {:ok, _pid} = Gnat.start_link(%{}, name: :async_pub_c3)

    # it should have used the new connection to publish our message and then we get it from our
    # anonymous connection
    assert_receive {:msg, %{topic: "async_pub_t3", body: "Ron Swanson", reply_to: "me"}}, 1000

    # The graceful shutdown should complete with a :normal reason and the process should be gone
    assert_receive {:shutdown, :ok}, 6000
    refute Process.alive?(async)
  end
end
