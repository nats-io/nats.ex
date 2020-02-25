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

  test "a graceful shutdown is attempted when being shutdown by a supervisor" do
    log = ExUnit.CaptureLog.capture_log(fn ->
      # start anonymous connection and subscribe to topic
      {:ok, pid} = Gnat.start_link()
      {:ok, _ref} = Gnat.sub(pid, self(), "async_pub_t3")

      # start async pub and send it a message before its connection is available
      {:ok, async} = Async.start_link(%{
        connection_name: :async_pub_c3,
        name: :async_pub_q3,
        graceful_shutdown_timeout: 5_000,
      })
      assert :ok = Async.pub(:async_pub_q3, "async_pub_t3", "Ron Swanson", reply_to: "me")

      # initiate a graceful shutdown - this must be done in another process because Supervisor.stop is synchronous
      test_pid = self()
      spawn_link(fn ->
        stopped = Supervisor.stop(async, :normal)
        Logger.info("Supervisor.stop returned #{inspect(stopped)}")
        Process.send(test_pid, {:shutdown, stopped}, [])
      end)
      :timer.sleep(15) # give a small amount of time for the shutdown signal to be received
      assert {:error, :shutting_down} = Async.pub(:async_pub_q3, "async_pub_t3", "Ron Swanson", reply_to: "me")

      # start the connection that our async pub wants to use
      {:ok, _pid} = Gnat.start_link(%{}, name: :async_pub_c3)

      # it should have used the new connection to publish our message and then we get it from our
      # anonymous connection
      assert_receive {:msg, %{topic: "async_pub_t3", body: "Ron Swanson", reply_to: "me"}}, 1000

      # The graceful shutdown should complete with a :normal reason and the process should be gone
      assert_receive {:shutdown, :ok}, 6000
      refute Process.alive?(async)
    end)
    refute log =~ "Timed out waiting for the async_pub_q3 async queue to drain"
  end

  test "logs an error if the queue does not drain before the timeout is reached" do
    log = ExUnit.CaptureLog.capture_log(fn ->
      # start anonymous connection and subscribe to topic
      {:ok, pid} = Gnat.start_link()
      {:ok, _ref} = Gnat.sub(pid, self(), "async_pub_t4")

      # start async pub and send it a message before its connection is available
      {:ok, async} = Async.start_link(%{
        connection_name: :async_pub_c4,
        name: :async_pub_q4,
        graceful_shutdown_timeout: 100
      })
      assert :ok = Async.pub(:async_pub_q4, "async_pub_t4", "Ron Swanson", reply_to: "me")

      # initiate a graceful shutdown - this must be done in another process because Supervisor.stop is synchronous
      test_pid = self()
      spawn_link(fn ->
        stopped = Supervisor.stop(async, :normal)
        Process.send(test_pid, {:shutdown, stopped}, [])
      end)
      :timer.sleep(15) # give a small amount of time for the shutdown signal to be received
      assert {:error, :shutting_down} = Async.pub(:async_pub_q4, "async_pub_t4", "Ron Swanson", reply_to: "me")

      # The graceful shutdown should complete with a :normal reason and the process should be gone
      assert_receive {:shutdown, :ok}, 1000
      refute Process.alive?(async)
    end)
    assert log =~ "Timed out waiting for the async_pub_q4 async queue to drain"
  end
end
