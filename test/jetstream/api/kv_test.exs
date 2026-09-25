defmodule Gnat.Jetstream.API.KVTest do
  use Gnat.Jetstream.ConnCase, min_server_version: "2.6.2"
  alias Gnat.Jetstream.API.KV
  alias Gnat.Jetstream.API.Stream

  @moduletag with_gnat: :gnat

  describe "create_bucket/3" do
    test "creates a bucket" do
      assert {:ok, %{config: config}} = KV.create_bucket(:gnat, "BUCKET_TEST")
      assert config.name == "KV_BUCKET_TEST"
      assert config.subjects == ["$KV.BUCKET_TEST.>"]
      assert config.max_msgs_per_subject == 1
      assert config.discard == :new
      assert config.allow_rollup_hdrs == true

      assert :ok = KV.delete_bucket(:gnat, "BUCKET_TEST")
    end

    test "creates a bucket with duplicate window < 2min" do
      assert {:ok, %{config: config}} = KV.create_bucket(:gnat, "TTL_TEST", ttl: 1_000_000_000)
      assert config.max_age == 1_000_000_000
      assert config.duplicate_window == 1_000_000_000

      assert :ok = KV.delete_bucket(:gnat, "TTL_TEST")
    end

    test "creates a bucket with duplicate window > 2min" do
      assert {:ok, %{config: config}} =
               KV.create_bucket(:gnat, "OTHER_TTL_TEST", ttl: 130_000_000_000)

      assert config.max_age == 130_000_000_000
      assert config.duplicate_window == 120_000_000_000

      assert :ok = KV.delete_bucket(:gnat, "OTHER_TTL_TEST")
    end

    @tag :message_ttl
    test "creates a bucket with limit_marker_ttl" do
      assert {:ok, %{config: config}} =
               KV.create_bucket(:gnat, "LIMIT_MARKER_TTL_TEST", limit_marker_ttl: 1_000_000_000)

      assert config.subject_delete_marker_ttl == 1_000_000_000

      assert :ok = KV.delete_bucket(:gnat, "LIMIT_MARKER_TTL_TEST")
    end
  end

  test "create_key/4 creates a key" do
    assert {:ok, _} = KV.create_bucket(:gnat, "KEY_CREATE_TEST")
    assert :ok = KV.create_key(:gnat, "KEY_CREATE_TEST", "foo", "bar")
    assert "bar" = KV.get_value(:gnat, "KEY_CREATE_TEST", "foo")
    assert :ok = KV.delete_bucket(:gnat, "KEY_CREATE_TEST")
  end

  test "create_key/4 returns error" do
    assert {:error, :timeout} = KV.create_key(:gnat, "KEY_CREATE_TEST", "foo", "bar", timeout: 1)
  end

  describe "conditional create" do
    setup do
      bucket = "CONDITIONAL_CREATE_TEST"
      assert {:ok, _} = KV.create_bucket(:gnat, bucket, history: 10)

      on_exit(fn ->
        {:ok, conn} = Gnat.start_link()
        KV.delete_bucket(conn, bucket)
        Gnat.stop(conn)
      end)

      %{bucket: bucket}
    end

    test "doesn't overwrite an existing key or advance its revision", %{bucket: bucket} do
      assert :ok = KV.create_key(:gnat, bucket, "foo", "original")

      assert {:ok, before} =
               Stream.get_message(:gnat, "KV_#{bucket}", %{last_by_subj: "$KV.#{bucket}.foo"})

      assert {:error, :key_exists} = KV.create_key(:gnat, bucket, "foo", "replacement")

      assert "original" = KV.get_value(:gnat, bucket, "foo")

      assert {:ok, ^before} =
               Stream.get_message(:gnat, "KV_#{bucket}", %{last_by_subj: "$KV.#{bucket}.foo"})

      assert :ok = KV.create_key(:gnat, bucket, "another", "new")
    end

    for operation <- [:delete_key, :purge_key] do
      test "recreates a key after #{operation}", %{bucket: bucket} do
        assert :ok = KV.create_key(:gnat, bucket, "foo", "original")
        assert :ok = apply(KV, unquote(operation), [:gnat, bucket, "foo"])
        assert :ok = KV.create_key(:gnat, bucket, "foo", "recreated")
        assert "recreated" = KV.get_value(:gnat, bucket, "foo")

        assert {:error, :key_exists} = KV.create_key(:gnat, bucket, "foo", "replacement")

        assert {:ok, %{seq: 3}} =
                 Stream.get_message(:gnat, "KV_#{bucket}", %{last_by_subj: "$KV.#{bucket}.foo"})
      end
    end

    for state <- [:new, :delete_key, :purge_key] do
      @tag initial_state: state
      test "only one concurrent creator succeeds for #{state}", %{
        bucket: bucket,
        initial_state: state
      } do
        if state != :new do
          assert :ok = KV.create_key(:gnat, bucket, "foo", "original")
          assert :ok = apply(KV, state, [:gnat, bucket, "foo"])
        end

        parent = self()

        tasks =
          for n <- 1..10 do
            Task.async(fn ->
              {:ok, conn} = Gnat.start_link()
              send(parent, {:ready, self()})

              receive do
                :create -> :ok
              end

              value = "creator-#{n}"
              result = KV.create_key(conn, bucket, "foo", value)
              Gnat.stop(conn)
              {value, result}
            end)
          end

        for %{pid: pid} <- tasks, do: assert_receive({:ready, ^pid})
        for %{pid: pid} <- tasks, do: send(pid, :create)
        results = Task.await_many(tasks)
        assert [{winner, :ok}] = Enum.filter(results, fn {_, result} -> result == :ok end)

        for {_, result} <- results, result != :ok do
          assert {:error, :key_exists} = result
        end

        assert ^winner = KV.get_value(:gnat, bucket, "foo")
        expected_revision = if state == :new, do: 1, else: 3

        assert {:ok, %{seq: ^expected_revision}} =
                 Stream.get_message(:gnat, "KV_#{bucket}", %{last_by_subj: "$KV.#{bucket}.foo"})
      end
    end

    test "returns server errors without reporting a successful create", %{bucket: bucket} do
      assert {:ok, %{config: config}} = KV.info(:gnat, bucket)
      assert {:ok, _} = Stream.update(:gnat, %{config | max_msg_size: 128})

      assert {:error, %{"code" => 400, "err_code" => 10054}} =
               KV.create_key(:gnat, bucket, "foo", String.duplicate("x", 256))

      assert {:error, %{"code" => 404}} = KV.get_value(:gnat, bucket, "foo")
    end

    test "an empty live value isn't a tombstone", %{bucket: bucket} do
      assert :ok = KV.put_value(:gnat, bucket, "foo", "")

      assert {:error, :key_exists} = KV.create_key(:gnat, bucket, "foo", "replacement")

      assert {:ok, %{seq: 1}} =
               Stream.get_message(:gnat, "KV_#{bucket}", %{last_by_subj: "$KV.#{bucket}.foo"})
    end

    test "returns a lookup error if the bucket disappears after a conflict", %{bucket: bucket} do
      assert :ok = KV.create_key(:gnat, bucket, "foo", "original")

      task =
        Task.async(fn ->
          receive do
            :create -> KV.create_key(:gnat, bucket, "foo", "replacement")
          end
        end)

      handler = make_ref()

      :ok =
        :telemetry.attach(
          handler,
          [:gnat, :request],
          &__MODULE__.pause_request/4,
          {task.pid, self(), "$KV.#{bucket}.foo"}
        )

      on_exit(fn -> :telemetry.detach(handler) end)

      send(task.pid, :create)
      assert_receive :request_completed
      assert :ok = KV.delete_bucket(:gnat, bucket)
      send(task.pid, :continue)
      assert {:error, %{"code" => 404, "err_code" => 10059}} = Task.await(task)
    end

    for operation <- [:delete_key, :purge_key] do
      test "doesn't overwrite a writer after reading a #{operation} tombstone", %{bucket: bucket} do
        assert :ok = KV.create_key(:gnat, bucket, "foo", "original")
        assert :ok = apply(KV, unquote(operation), [:gnat, bucket, "foo"])

        task =
          Task.async(fn ->
            receive do
              :create -> KV.create_key(:gnat, bucket, "foo", "loser")
            end
          end)

        handler = make_ref()

        :ok =
          :telemetry.attach(
            handler,
            [:gnat, :request],
            &__MODULE__.pause_request/4,
            {task.pid, self(), "$JS.API.STREAM.MSG.GET.KV_#{bucket}"}
          )

        on_exit(fn -> :telemetry.detach(handler) end)

        send(task.pid, :create)
        assert_receive :request_completed
        assert :ok = KV.create_key(:gnat, bucket, "foo", "winner")
        send(task.pid, :continue)
        assert {:error, :key_exists} = Task.await(task)
        assert "winner" = KV.get_value(:gnat, bucket, "foo")
      end
    end

    @tag :message_ttl
    test "recreates a key over a server-generated expiry marker", %{bucket: bucket} do
      assert {:ok, %{config: config}} = KV.info(:gnat, bucket)

      assert {:ok, _} =
               Stream.update(:gnat, %{
                 config
                 | max_age: 1_000_000_000,
                   duplicate_window: 1_000_000_000,
                   subject_delete_marker_ttl: 30_000_000_000
               })

      parent = self()

      assert {:ok, watcher} =
               KV.watch(:gnat, bucket, fn action, key, value ->
                 send(parent, {action, key, value})
               end)

      assert :ok = KV.create_key(:gnat, bucket, "foo", "original")
      assert_receive {:key_added, "foo", "original"}
      assert_receive {:key_purged, "foo", ""}, 2_000
      assert :ok = KV.create_key(:gnat, bucket, "foo", "recreated")
      assert "recreated" = KV.get_value(:gnat, bucket, "foo")
      KV.unwatch(watcher)
    end
  end

  test "create_key rejects malformed and unrelated publish acknowledgements" do
    subject = "$KV.INVALID_CREATE_ACK.foo"
    assert {:ok, sid} = Gnat.sub(:gnat, self(), subject)

    for response <- [
          "not json",
          "{}",
          ~s({"stream":"other","seq":1}),
          ~s({"stream":"KV_INVALID_CREATE_ACK","seq":0})
        ] do
      task = Task.async(fn -> KV.create_key(:gnat, "INVALID_CREATE_ACK", "foo", "value") end)
      assert_receive {:msg, %{reply_to: reply, headers: headers}}
      assert {"nats-expected-last-subject-sequence", "0"} in headers
      :ok = Gnat.pub(:gnat, reply, response)
      assert {:error, :invalid_publish_ack} = Task.await(task)
    end

    :ok = Gnat.unsub(:gnat, sid)
  end

  # No JetStream domain by this name exists on the test server, so only our own
  # subscription answers API requests routed to it.
  @unserved_domain "UNSERVED_TEST_DOMAIN"

  test "create_key rejects malformed tombstone lookup replies" do
    tombstone = Base.encode64("NATS/1.0\r\nKV-Operation: DEL\r\n\r\n")

    responses = [
      "not json",
      "{}",
      "[]",
      ~s({"message":null}),
      ~s({"message":{}}),
      Jason.encode!(%{message: %{hdrs: tombstone}}),
      Jason.encode!(%{message: %{seq: "2", hdrs: tombstone}}),
      Jason.encode!(%{message: %{seq: 0, hdrs: tombstone}}),
      Jason.encode!(%{message: %{seq: -1, hdrs: tombstone}}),
      Jason.encode!(%{message: %{seq: 2, hdrs: nil}}),
      Jason.encode!(%{message: %{seq: 2, hdrs: 123}}),
      Jason.encode!(%{message: %{seq: 2, hdrs: "!"}}),
      Jason.encode!(%{message: %{seq: 2, hdrs: Base.encode64("invalid headers")}})
    ]

    {publish_subject, lookup_subject, sids} = fake_bucket("INVALID_LOOKUP")

    for response <- responses do
      task =
        Task.async(fn ->
          KV.create_key(:gnat, "INVALID_LOOKUP", "foo", "value", domain: @unserved_domain)
        end)

      reply_to_request(publish_subject, Jason.encode!(%{error: %{code: 400, err_code: 10071}}))
      reply_to_request(lookup_subject, response)
      assert {:error, :invalid_lookup_response} = Task.await(task)
    end

    for sid <- sids, do: :ok = Gnat.unsub(:gnat, sid)
  end

  test "create_key maps replicated-stream sequence conflicts to :key_exists" do
    {publish_subject, lookup_subject, sids} = fake_bucket("REPLICATED_CONFLICT")

    task =
      Task.async(fn ->
        KV.create_key(:gnat, "REPLICATED_CONFLICT", "foo", "value", domain: @unserved_domain)
      end)

    reply_to_request(publish_subject, Jason.encode!(%{error: %{code: 400, err_code: 10164}}))

    lookup = reply_to_request(lookup_subject, Jason.encode!(%{message: %{seq: 7}}))
    assert %{"last_by_subj" => ^publish_subject} = Jason.decode!(lookup)

    assert {:error, :key_exists} = Task.await(task)

    for sid <- sids, do: :ok = Gnat.unsub(:gnat, sid)
  end

  # Subscribes to the publish and tombstone-lookup subjects for a bucket that
  # doesn't exist, so the test can play the server's part in the exchange.
  defp fake_bucket(bucket) do
    publish_subject = "$KV.#{bucket}.foo"
    lookup_subject = "$JS.#{@unserved_domain}.API.STREAM.MSG.GET.KV_#{bucket}"
    assert {:ok, publish_sid} = Gnat.sub(:gnat, self(), publish_subject)
    assert {:ok, lookup_sid} = Gnat.sub(:gnat, self(), lookup_subject)
    {publish_subject, lookup_subject, [publish_sid, lookup_sid]}
  end

  # Waits for a request on `subject`, publishes `response` to its reply inbox and
  # returns the request body.
  defp reply_to_request(subject, response) do
    assert_receive {:msg, %{topic: ^subject, reply_to: reply_to, body: body}}
    :ok = Gnat.pub(:gnat, reply_to, response)
    body
  end

  def pause_request(_event, _measurements, %{topic: topic}, {caller, parent, topic})
      when self() == caller do
    send(parent, :request_completed)

    receive do
      :continue -> :ok
    after
      5_000 -> raise "request wasn't resumed"
    end
  end

  def pause_request(_event, _measurements, _metadata, _config), do: :ok

  test "delete_key/3 deletes a key" do
    assert {:ok, _} = KV.create_bucket(:gnat, "KEY_DELETE_TEST")
    assert :ok = KV.create_key(:gnat, "KEY_DELETE_TEST", "foo", "bar")
    assert :ok = KV.delete_key(:gnat, "KEY_DELETE_TEST", "foo")
    assert KV.get_value(:gnat, "KEY_DELETE_TEST", "foo") == nil
    assert :ok = KV.delete_bucket(:gnat, "KEY_DELETE_TEST")
  end

  test "delete_key/3 returns error" do
    assert {:error, :timeout} = KV.delete_key(:gnat, "KEY_DELETE_TEST", "foo", timeout: 1)
  end

  test "purge_key/3 purges a key" do
    assert {:ok, _} = KV.create_bucket(:gnat, "KEY_PURGE_TEST")
    assert :ok = KV.create_key(:gnat, "KEY_PURGE_TEST", "foo", "bar")
    assert :ok = KV.purge_key(:gnat, "KEY_PURGE_TEST", "foo")
    assert KV.get_value(:gnat, "KEY_PURGE_TEST", "foo") == nil
    assert :ok = KV.delete_bucket(:gnat, "KEY_PURGE_TEST")
  end

  test "purge_key/3 returns error" do
    assert {:error, :timeout} = KV.purge_key(:gnat, "KEY_PURGE_TEST", "foo", timeout: 1)
  end

  test "put_value/4 updates a key" do
    assert {:ok, _} = KV.create_bucket(:gnat, "KEY_PUT_TEST")
    assert :ok = KV.create_key(:gnat, "KEY_PUT_TEST", "foo", "bar")
    assert :ok = KV.put_value(:gnat, "KEY_PUT_TEST", "foo", "baz")
    assert "baz" = KV.get_value(:gnat, "KEY_PUT_TEST", "foo")
    assert :ok = KV.delete_bucket(:gnat, "KEY_PUT_TEST")
  end

  test "put_value/4 returns error" do
    assert {:error, :timeout} = KV.put_value(:gnat, "KEY_PUT_TEST", "foo", "baz", timeout: 1)
  end

  @tag :message_ttl
  test "detects key removed based on limit_marker_ttl" do
    assert {:ok, _} =
             KV.create_bucket(:gnat, "LIMIT_MARKER_TTL_TEST",
               limit_marker_ttl: 1_000_000_000,
               ttl: 1_000_000_000
             )

    test_pid = self()

    {:ok, watcher_pid} =
      KV.watch(:gnat, "LIMIT_MARKER_TTL_TEST", fn action, key, value ->
        send(test_pid, {action, key, value})
      end)

    KV.put_value(:gnat, "LIMIT_MARKER_TTL_TEST", "foo", "bar")
    assert_receive({:key_added, "foo", "bar"})

    # a limit marker is a server-generated purge, matching the official clients
    assert_receive({:key_purged, "foo", ""}, 1500)

    KV.unwatch(watcher_pid)
    assert :ok = KV.delete_bucket(:gnat, "LIMIT_MARKER_TTL_TEST")
  end

  describe "watch/3" do
    setup do
      bucket = "KEY_WATCH_TEST"
      {:ok, _} = KV.create_bucket(:gnat, bucket)
      %{bucket: bucket}
    end

    test "detects key added and removed keys", %{bucket: bucket} do
      test_pid = self()

      {:ok, watcher_pid} =
        KV.watch(:gnat, bucket, fn action, key, value ->
          send(test_pid, {action, key, value})
        end)

      KV.put_value(:gnat, bucket, "foo", "bar")
      assert_receive({:key_added, "foo", "bar"})

      KV.put_value(:gnat, bucket, "baz", "quz")
      assert_receive({:key_added, "baz", "quz"})

      KV.delete_key(:gnat, bucket, "baz")
      # key deletions don't carry the data removed
      assert_receive({:key_deleted, "baz", ""})

      KV.put_value(:gnat, bucket, "foo", "buzz")
      assert_receive({:key_added, "foo", "buzz"})

      KV.purge_key(:gnat, bucket, "foo")
      assert_receive({:key_purged, "foo", ""})

      KV.unwatch(watcher_pid)

      :ok = KV.delete_bucket(:gnat, bucket)
    end
  end

  describe "contents/2" do
    setup do
      bucket = "KEY_LIST_TEST"
      {:ok, _} = KV.create_bucket(:gnat, bucket)
      %{bucket: bucket}
    end

    test "provides all keys", %{bucket: bucket} do
      KV.put_value(:gnat, bucket, "foo", "bar")
      KV.put_value(:gnat, bucket, "baz", "quz")
      assert {:ok, %{"foo" => "bar", "baz" => "quz"}} == KV.contents(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "deleted keys not included", %{bucket: bucket} do
      KV.put_value(:gnat, bucket, "foo", "bar")
      KV.put_value(:gnat, bucket, "baz", "quz")
      KV.delete_key(:gnat, bucket, "baz")
      assert {:ok, %{"foo" => "bar"}} == KV.contents(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "purged keys not included", %{bucket: bucket} do
      KV.put_value(:gnat, bucket, "foo", "bar")
      KV.put_value(:gnat, bucket, "baz", "quz")
      KV.purge_key(:gnat, bucket, "baz")
      assert {:ok, %{"foo" => "bar"}} == KV.contents(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "updated keys use most recent", %{bucket: bucket} do
      :ok = KV.delete_bucket(:gnat, bucket)
      {:ok, _} = KV.create_bucket(:gnat, bucket, history: 5)
      KV.put_value(:gnat, bucket, "foo", "bar")
      KV.put_value(:gnat, bucket, "foo", "baz")
      assert {:ok, %{"foo" => "baz"}} == KV.contents(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "empty for no keys", %{bucket: bucket} do
      assert {:ok, %{}} == KV.contents(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "error tuple if problem", %{bucket: bucket} do
      assert {:error, _message} = KV.contents(:gnat, "NOT_REAL_BUCKET")
      :ok = KV.delete_bucket(:gnat, bucket)
    end
  end

  describe "keys/2" do
    setup do
      bucket = "KEY_KEYS_TEST"
      {:ok, _} = KV.create_bucket(:gnat, bucket)
      %{bucket: bucket}
    end

    test "provides all keys", %{bucket: bucket} do
      KV.put_value(:gnat, bucket, "foo", "bar")
      KV.put_value(:gnat, bucket, "baz", "quz")
      KV.put_value(:gnat, bucket, "alpha", "beta")
      assert {:ok, ["alpha", "baz", "foo"]} == KV.keys(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "deleted keys not included", %{bucket: bucket} do
      KV.put_value(:gnat, bucket, "foo", "bar")
      KV.put_value(:gnat, bucket, "baz", "quz")
      KV.put_value(:gnat, bucket, "alpha", "beta")
      KV.delete_key(:gnat, bucket, "baz")
      assert {:ok, ["alpha", "foo"]} == KV.keys(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "purged keys not included", %{bucket: bucket} do
      KV.put_value(:gnat, bucket, "foo", "bar")
      KV.put_value(:gnat, bucket, "baz", "quz")
      KV.purge_key(:gnat, bucket, "foo")
      assert {:ok, ["baz"]} == KV.keys(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "updated keys only appear once", %{bucket: bucket} do
      :ok = KV.delete_bucket(:gnat, bucket)
      {:ok, _} = KV.create_bucket(:gnat, bucket, history: 5)
      KV.put_value(:gnat, bucket, "foo", "bar")
      KV.put_value(:gnat, bucket, "foo", "baz")
      KV.put_value(:gnat, bucket, "foo", "qux")
      assert {:ok, ["foo"]} == KV.keys(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "empty list for no keys", %{bucket: bucket} do
      assert {:ok, []} == KV.keys(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "keys are sorted alphabetically", %{bucket: bucket} do
      KV.put_value(:gnat, bucket, "zebra", "value1")
      KV.put_value(:gnat, bucket, "apple", "value2")
      KV.put_value(:gnat, bucket, "middle", "value3")
      assert {:ok, ["apple", "middle", "zebra"]} == KV.keys(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "error tuple if bucket does not exist", %{bucket: bucket} do
      assert {:error, _message} = KV.keys(:gnat, "NOT_REAL_BUCKET")
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "handles keys with special characters", %{bucket: bucket} do
      KV.put_value(:gnat, bucket, "key.with.dots", "value1")
      KV.put_value(:gnat, bucket, "key-with-dashes", "value2")
      KV.put_value(:gnat, bucket, "key_with_underscores", "value3")

      assert {:ok, ["key-with-dashes", "key.with.dots", "key_with_underscores"]} ==
               KV.keys(:gnat, bucket)

      :ok = KV.delete_bucket(:gnat, bucket)
    end
  end

  describe "list_buckets/2" do
    test "list buckets when none exists" do
      assert {:ok, []} = KV.list_buckets(:gnat)
    end

    test "list buckets properly" do
      assert {:ok, %{config: _config}} = KV.create_bucket(:gnat, "TEST_BUCKET_1")
      assert {:ok, %{config: _config}} = KV.create_bucket(:gnat, "TEST_BUCKET_2")
      assert {:ok, ["TEST_BUCKET_1", "TEST_BUCKET_2"]} = KV.list_buckets(:gnat)
      :ok = KV.delete_bucket(:gnat, "TEST_BUCKET_1")
      :ok = KV.delete_bucket(:gnat, "TEST_BUCKET_2")
    end

    test "ignore streams that are not buckets" do
      assert {:ok, %{config: _config}} = KV.create_bucket(:gnat, "TEST_BUCKET_1")

      stream = %Stream{
        name: "TEST_STREAM_1",
        subjects: ["TEST_STREAM_1.subject1", "TEST_STREAM_1.subject2"]
      }

      assert {:ok, _response} = Stream.create(:gnat, stream)
      assert {:ok, ["TEST_BUCKET_1"]} = KV.list_buckets(:gnat)
      :ok = KV.delete_bucket(:gnat, "TEST_BUCKET_1")
    end
  end

  describe "info/3" do
    test "returns bucket info" do
      assert {:ok, _} = KV.create_bucket(:gnat, "TEST_BUCKET_1")
      assert {:ok, %{config: %{name: "KV_TEST_BUCKET_1"}}} = KV.info(:gnat, "TEST_BUCKET_1")
      :ok = KV.delete_bucket(:gnat, "TEST_BUCKET_1")
      assert {:error, %{"code" => 404}} = KV.info(:gnat, "NOT_A_BUCKET")
    end
  end
end
