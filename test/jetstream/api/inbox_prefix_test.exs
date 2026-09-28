defmodule Gnat.Jetstream.API.InboxPrefixTest do
  use Gnat.Jetstream.ConnCase, min_server_version: "2.6.2"
  alias Gnat.Jetstream.API.{Object, Util}

  @restricted %{port: 4228, username: "inbox", password: "prefix", inbox_prefix: "custom_inbox."}

  describe "a user that may only subscribe under a custom inbox prefix" do
    @describetag :multi_server

    setup do
      conn = connect(@restricted)
      bucket = Util.nuid()
      {:ok, _info} = Object.create_bucket(conn, bucket)
      on_exit(fn -> cleanup_bucket(bucket, @restricted) end)
      %{conn: conn, bucket: bucket}
    end

    test "can get and list objects", %{conn: conn, bucket: bucket} do
      {:ok, io} = File.open("README.md", [:read])
      assert {:ok, _meta} = Object.put(conn, bucket, "readme", io)

      parent = self()
      assert :ok = Object.get(conn, bucket, "readme", &send(parent, {:chunk, &1}))
      assert_received {:chunk, _chunk}

      assert {:ok, [%{name: "readme"}]} = Object.list(conn, bucket)
    end
  end

  describe "push consumer deliver subjects" do
    @describetag with_gnat: :gnat

    setup do
      bucket = Util.nuid()
      {:ok, _info} = Object.create_bucket(:gnat, bucket)
      {:ok, io} = File.open("README.md", [:read])
      {:ok, _meta} = Object.put(:gnat, bucket, "readme", io)
      on_exit(fn -> cleanup_bucket(bucket, %{}) end)
      %{bucket: bucket}
    end

    test "follow the connection's inbox prefix", %{bucket: bucket} do
      conn = connect(%{inbox_prefix: "conn_prefix."})
      spy("conn_prefix.>")

      assert :ok = Object.get(conn, bucket, "readme", fn _chunk -> :ok end)
      assert_delivered_under("conn_prefix.")

      assert {:ok, [_meta]} = Object.list(conn, bucket)
      assert_delivered_under("conn_prefix.")
    end

    test "follow an explicit inbox_prefix option", %{bucket: bucket} do
      spy("explicit_prefix.>")

      assert :ok =
               Object.get(:gnat, bucket, "readme", fn _chunk -> :ok end,
                 inbox_prefix: "explicit_prefix."
               )

      assert_delivered_under("explicit_prefix.")

      assert {:ok, [_meta]} = Object.list(:gnat, bucket, inbox_prefix: "explicit_prefix.")
      assert_delivered_under("explicit_prefix.")
    end
  end

  defp connect(settings) do
    start_supervised!(%{id: make_ref(), start: {Gnat, :start_link, [settings]}})
  end

  # Forwards the subjects of messages delivered under `subject`, seen by a separate process on
  # a separate connection, as `{:spied, topic}`.
  defp spy(subject) do
    parent = self()
    spy_conn = connect(%{})

    spy =
      spawn_link(fn ->
        {:ok, _sid} = Gnat.sub(spy_conn, self(), subject)
        send(parent, :spying)
        forward(parent)
      end)

    assert_receive :spying
    # the subscription is registered once a round trip on the same connection completes
    {:ok, _reply} = Gnat.request(spy_conn, "$JS.API.INFO", "")
    spy
  end

  defp forward(parent) do
    receive do
      {:msg, %{topic: topic}} -> send(parent, {:spied, topic})
    end

    forward(parent)
  end

  # Waits for an object store message at the spy. Push consumer deliveries keep the stream
  # subject of the message, so receiving one proves it was delivered to a subject under
  # `prefix`, the only subjects the spy subscribes to; request replies are skipped.
  defp assert_delivered_under(prefix) do
    receive do
      {:spied, "$O." <> _subject} -> :ok
      {:spied, _reply} -> assert_delivered_under(prefix)
    after
      1_000 -> flunk("nothing was delivered under #{prefix}")
    end
  end

  defp cleanup_bucket(bucket, settings) do
    {:ok, conn} = Gnat.start_link(settings)
    :ok = Object.delete_bucket(conn, bucket)
    Gnat.stop(conn)
  end
end
