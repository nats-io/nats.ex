defmodule Gnat.Jetstream.API.KV.WatcherTest do
  use Gnat.Jetstream.ConnCase, min_server_version: "2.6.2"

  alias Gnat.Jetstream.API.KV

  @moduletag with_gnat: :gnat

  describe "status messages" do
    setup do
      bucket = "WATCHER_STATUS_TEST"
      {:ok, _} = KV.create_bucket(:gnat, bucket)

      on_exit(fn ->
        {:ok, pid} = Gnat.start_link()
        :ok = KV.delete_bucket(pid, bucket)
        Gnat.stop(pid)
      end)

      test_pid = self()

      {:ok, watcher} =
        KV.watch(:gnat, bucket, fn action, key, value ->
          send(test_pid, {action, key, value})
        end)

      on_exit(fn ->
        if Process.alive?(watcher), do: KV.unwatch(watcher)
      end)

      %{watcher: watcher}
    end

    test "409 status messages are dropped", %{watcher: watcher} do
      send(
        watcher,
        {:msg, %{status: "409", description: "Leadership Change", body: "", topic: "ignored"}}
      )

      refute_receive {:key_added, _, _}, 100
      refute_receive {:key_deleted, _, _}, 100
      assert Process.alive?(watcher)
    end

    test "100 idle heartbeat (no reply_to) is dropped", %{watcher: watcher} do
      send(watcher, {:msg, %{status: "100", body: "", topic: "ignored"}})

      refute_receive {:key_added, _, _}, 100
      assert Process.alive?(watcher)
    end

    test "100 flow-control message is answered with an empty publish", %{watcher: watcher} do
      reply_to = "_WATCHER_TEST.fc.#{System.unique_integer([:positive])}"
      {:ok, _sub} = Gnat.sub(:gnat, self(), reply_to)

      send(
        watcher,
        {:msg,
         %{
           status: "100",
           description: "FlowControl Request",
           body: "",
           reply_to: reply_to,
           topic: "ignored"
         }}
      )

      assert_receive {:msg, %{topic: ^reply_to, body: ""}}, 500
    end
  end
end
