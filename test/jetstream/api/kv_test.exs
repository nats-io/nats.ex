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
