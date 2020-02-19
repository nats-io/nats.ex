defmodule Gnat.AsyncPubTest do
  use ExUnit.Case, async: true
  alias Gnat.AsyncPub

  test "messages are eventually published" do
    {:ok, _pid} = Gnat.start_link(%{}, name: :async_pub_c1)
    {:ok, _pid} = AsyncPub.start_link(%{connection_name: :async_pub_c1}, name: :async_pub_q1)
    {:ok, _ref} = Gnat.sub(:async_pub_c1, self(), "async_pub_t1")
    assert :ok = AsyncPub.pub(:async_pub_q1, "async_pub_t1", "Ahoy there, Matey")
    assert_receive {:msg, %{topic: "async_pub_t1", body: "Ahoy there, Matey"}}, 1000
  end

  test "messages can be published without the connection being available" do
    # start anonymous connection and subscribe to topic
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "async_pub_t2")

    # start async pub and send it a message before its connection is available
    {:ok, _pid} = AsyncPub.start_link(%{connection_name: :async_pub_c2}, name: :async_pub_q2)
    assert :ok = AsyncPub.pub(:async_pub_q2, "async_pub_t2", "Ron Swanson", reply_to: "me")

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
    {:ok, queue} = AsyncPub.start_link(%{connection_name: :async_pub_c2}, name: :async_pub_q2)
    assert :ok = AsyncPub.pub(:async_pub_q2, "async_pub_t3", "Ron Swanson", reply_to: "me")

    # initiate a graceful shutdown
    Process.send(queue, :shutdown, []) # why does Process.exit kill the test? It should be trapped by
    assert {:error, :shutting_down} = AsyncPub.pub(:async_pub_q2, "async_pub_t3", "Ron Swanson", reply_to: "me")

    # start the connection that our async pub wants to use
    {:ok, _pid} = Gnat.start_link(%{}, name: :async_pub_c2)

    # it should have used the new connection to publish our message and then we get it from our
    # anonymous connection
    assert_receive {:msg, %{topic: "async_pub_t3", body: "Ron Swanson", reply_to: "me"}}, 1000

    # The graceful shutdown should be complete at this point
    refute Process.alive?(queue)
  end
end
