defmodule Gnat.Jetstream.API.ObjectTest do
  use Gnat.Jetstream.ConnCase, min_server_version: "2.6.2"
  alias Gnat.Jetstream.API.{Consumer, Object, Stream}
  import Gnat.Jetstream.API.Util, only: [nuid: 0]

  @moduletag with_gnat: :gnat
  @readme_path Path.join([Path.dirname(__DIR__), "..", "..", "README.md"])

  describe "create_bucket/3" do
    test "create/delete a bucket" do
      assert {:ok, %{config: config}} = Object.create_bucket(:gnat, "MY-STORE")
      assert config.name == "OBJ_MY-STORE"
      assert config.max_age == 0
      assert config.max_bytes == -1
      assert config.storage == :file
      assert config.allow_rollup_hdrs == true

      assert config.subjects == [
               "$O.MY-STORE.C.>",
               "$O.MY-STORE.M.>"
             ]

      assert :ok = Object.delete_bucket(:gnat, "MY-STORE")
    end

    test "creating a bucket with TTL" do
      bucket = nuid()
      # 10s in nanoseconds
      ttl = 10 * 1_000_000_000
      assert {:ok, %{config: config}} = Object.create_bucket(:gnat, bucket, ttl: ttl)
      assert config.max_age == ttl

      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "bucket names are validated" do
      assert {:error, "invalid bucket name"} = Object.create_bucket(:gnat, "")
      assert {:error, "invalid bucket name"} = Object.create_bucket(:gnat, "MY.STORE")
      assert {:error, "invalid bucket name"} = Object.create_bucket(:gnat, "(*!&@($%*&))")
    end
  end

  describe "delete_bucket/2" do
    test "create/delete a bucket" do
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, "MY-STORE")
      assert :ok = Object.delete_bucket(:gnat, "MY-STORE")
    end
  end

  describe "delete/3" do
    test "delete an object" do
      bucket = nuid()
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, bucket)
      {:ok, _} = put_filepath(@readme_path, bucket, "README.md")
      {:ok, _} = put_filepath(@readme_path, bucket, "OTHER.md")
      assert :ok = Object.delete(:gnat, bucket, "README.md")

      assert {:ok, objects} = Object.list(:gnat, bucket)
      assert Enum.count(objects) == 1
      assert Enum.map(objects, & &1.name) == ["OTHER.md"]
      assert {:ok, objects} = Object.list(:gnat, bucket, show_deleted: true)
      assert Enum.count(objects) == 2
      assert Enum.map(objects, & &1.name) |> Enum.sort() == ["OTHER.md", "README.md"]

      assert :ok = Object.delete_bucket(:gnat, bucket)
    end
  end

  describe "get/4" do
    test "verifies empty objects without allocating a consumer" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      assert {:ok, _} = put_binary("", bucket, "empty")
      assert :ok = Object.get(:gnat, bucket, "empty", fn _ -> flunk("unexpected chunk") end)
      assert_read_resources_released(bucket)
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "rejects a digest mismatch and releases read resources" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      assert {:ok, meta} = put_binary("data", bucket, "corrupt")

      replace_meta(%{
        meta
        | digest: "SHA-256=" <> Base.url_encode64(:crypto.hash(:sha256, "other"))
      })

      assert {:error, :digest_mismatch} = Object.get(:gnat, bucket, meta.name, fn _ -> :ok end)
      assert_read_resources_released(bucket)
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "rejects invalid digests before delivering data" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      assert {:ok, meta} = put_binary("data", bucket, "invalid-digest")

      for digest <- ["SHA-512=abc", "SHA-256=???", "SHA-256=YQ=="] do
        replace_meta(%{meta | digest: digest})

        assert {:error, :invalid_digest} =
                 Object.get(:gnat, bucket, meta.name, fn _ -> flunk("unexpected chunk") end)

        assert_read_resources_released(bucket)
      end

      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "verifies the object size" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      assert {:ok, meta} = put_binary("data", bucket, "wrong-size")

      for size <- [3, 5] do
        replace_meta(%{meta | size: size})
        assert {:error, :size_mismatch} = Object.get(:gnat, bucket, meta.name, fn _ -> :ok end)
        assert_read_resources_released(bucket)
      end

      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "releases subscriptions and consumers when a callback raises or exits" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket, max_chunk_size: 1024)
      assert {:ok, meta} = put_binary(:binary.copy("a", 3000), bucket, "callbacks")

      assert_raise RuntimeError, "callback failed", fn ->
        Object.get(:gnat, bucket, meta.name, fn _ -> raise "callback failed" end)
      end

      assert_read_resources_released(bucket)
      refute_received {:msg, _}

      assert catch_exit(Object.get(:gnat, bucket, meta.name, fn _ -> exit(:callback_failed) end)) ==
               :callback_failed

      assert_read_resources_released(bucket)
      refute_received {:msg, _}
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "returns consumer creation errors and releases the subscription" do
      bucket = nuid()
      config = create_limited_bucket(bucket)
      assert {:ok, meta} = put_binary("data", bucket, "no-consumers")
      assert {:ok, consumer} = Consumer.create(:gnat, %Consumer{stream_name: config.name})

      assert {:error, %{"code" => 400}} = Object.get(:gnat, bucket, meta.name, fn _ -> :ok end)
      assert {:ok, 1} = Gnat.active_subscriptions(:gnat)
      assert :ok = Consumer.delete(:gnat, config.name, consumer.name)
    end

    test "missing chunks time out and release read resources" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      assert {:ok, meta} = put_binary("data", bucket, "missing")

      assert :ok =
               Stream.purge(:gnat, "OBJ_#{bucket}", nil, %{filter: "$O.#{bucket}.C.#{meta.nuid}"})

      assert {:error, :timeout_waiting_for_messages} =
               Object.get(:gnat, bucket, meta.name, fn _ -> flunk("unexpected chunk") end,
                 timeout: 50
               )

      assert_read_resources_released(bucket)
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "data chunks reset the inactivity timeout after callbacks" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket, max_chunk_size: 1024)
      cleanup_bucket_on_exit(bucket)
      assert {:ok, meta} = put_binary(:binary.copy("a", 3000), bucket, "progress")

      assert :ok =
               Object.get(:gnat, bucket, meta.name, fn _ -> Process.sleep(150) end, timeout: 100)

      assert_read_resources_released(bucket)
    end

    @tag timeout: 20_000
    test "the default permits progressing reads lasting longer than ten seconds" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket, max_chunk_size: 1024)
      cleanup_bucket_on_exit(bucket)
      assert {:ok, meta} = put_binary(:binary.copy("a", 1500), bucket, "long-read")

      assert :ok =
               Object.get(:gnat, bucket, meta.name, fn chunk ->
                 if byte_size(chunk) == 1024, do: Process.sleep(10_100)
               end)

      assert_read_resources_released(bucket)
    end

    test "the optional total deadline expires despite data progress" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket, max_chunk_size: 1024)
      cleanup_bucket_on_exit(bucket)
      assert {:ok, meta} = put_binary(:binary.copy("a", 3000), bucket, "total-timeout")

      assert {:error, :timeout_waiting_for_messages} =
               Object.get(:gnat, bucket, meta.name, fn _ -> Process.sleep(150) end,
                 timeout: 1000,
                 total_timeout: 100
               )

      assert_read_resources_released(bucket)
    end

    test "the inactivity timeout can expire before the total deadline" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      cleanup_bucket_on_exit(bucket)
      assert {:ok, meta} = put_binary("data", bucket, "idle-timeout")

      assert :ok =
               Stream.purge(:gnat, "OBJ_#{bucket}", nil, %{filter: "$O.#{bucket}.C.#{meta.nuid}"})

      started = System.monotonic_time(:millisecond)

      assert {:error, :timeout_waiting_for_messages} =
               Object.get(:gnat, bucket, meta.name, fn _ -> flunk("unexpected chunk") end,
                 timeout: 50,
                 total_timeout: 5000
               )

      assert System.monotonic_time(:millisecond) - started < 3000
      assert_read_resources_released(bucket)
    end

    @tag timeout: 8000
    test "heartbeats don't extend the inactivity timeout" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      assert {:ok, meta} = put_binary("data", bucket, "missing")

      assert :ok =
               Stream.purge(:gnat, "OBJ_#{bucket}", nil, %{filter: "$O.#{bucket}.C.#{meta.nuid}"})

      assert {:error, :timeout_waiting_for_messages} =
               Object.get(:gnat, bucket, meta.name, fn _ -> flunk("unexpected chunk") end,
                 timeout: 5500
               )

      assert_read_resources_released(bucket)
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "retrieves and object chunk-by-chunk" do
      nuid = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, nuid)
      readme_content = File.read!(@readme_path)
      assert {:ok, _meta} = put_filepath(@readme_path, nuid, "README.md")

      assert :ok =
               Object.get(:gnat, nuid, "README.md", fn chunk ->
                 assert chunk == readme_content
                 send(self(), :got_chunk)
               end)

      assert_received :got_chunk

      :ok = Object.delete_bucket(:gnat, nuid)
    end
  end

  describe "info/3" do
    test "lookup meta information about an object" do
      assert {:ok, %{config: _stream}} = Object.create_bucket(:gnat, "INF")
      assert {:ok, io} = File.open(@readme_path, [:read])
      assert {:ok, initial_meta} = Object.put(:gnat, "INF", "README.md", io)

      assert {:ok, lookup_meta} = Object.info(:gnat, "INF", "README.md")
      assert lookup_meta == initial_meta

      assert :ok = Object.delete_bucket(:gnat, "INF")
    end
  end

  describe "list/3" do
    test "a receive timeout releases subscriptions and consumers" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      assert {:ok, _} = put_binary("data", bucket, "timeout")

      assert {:error, :timeout_waiting_for_messages} = Object.list(:gnat, bucket, timeout: 0)
      assert_read_resources_released(bucket)
      refute_received {:msg, _}
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "the optional total deadline applies to metadata listing" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      cleanup_bucket_on_exit(bucket)
      assert {:ok, _} = put_binary("data", bucket, "total-timeout")

      assert {:error, :timeout_waiting_for_messages} =
               Object.list(:gnat, bucket, total_timeout: 0)

      assert_read_resources_released(bucket)
      refute_received {:msg, _}
    end

    test "consumer creation errors release the subscription" do
      bucket = nuid()
      config = create_limited_bucket(bucket)
      assert {:ok, consumer} = Consumer.create(:gnat, %Consumer{stream_name: config.name})

      assert {:error, %{"code" => 400}} = Object.list(:gnat, bucket)
      assert {:ok, 1} = Gnat.active_subscriptions(:gnat)
      assert :ok = Consumer.delete(:gnat, config.name, consumer.name)
    end

    test "invalid metadata returns an error and releases read resources" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      assert {:ok, _} = Gnat.request(:gnat, "$O.#{bucket}.M.bad", "not json")
      assert {:error, :invalid_object_metadata} = Object.list(:gnat, bucket)
      assert_read_resources_released(bucket)
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "list an empty bucket" do
      bucket = nuid()
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, bucket)
      assert {:ok, []} = Object.list(:gnat, bucket)
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "list a bucket with two files" do
      bucket = nuid()
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, bucket)
      assert {:ok, io} = File.open(@readme_path, [:read])
      assert {:ok, _object} = Object.put(:gnat, bucket, "README.md", io)
      assert {:ok, io} = File.open(@readme_path, [:read])
      assert {:ok, _object} = Object.put(:gnat, bucket, "SOMETHING.md", io)

      assert {:ok, objects} = Object.list(:gnat, bucket)
      [readme, something] = Enum.sort_by(objects, & &1.name)
      assert readme.name == "README.md"
      assert readme.size == something.size
      assert readme.digest == something.digest

      assert :ok = Object.delete_bucket(:gnat, bucket)
    end
  end

  describe "put/4" do
    test "respects the connection's advertised payload limit", %{conn: conn} do
      :sys.replace_state(conn, fn state ->
        put_in(state.server_info.max_payload, 2048)
      end)

      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket)
      assert {:ok, meta} = put_binary(:binary.copy("a", 5000), bucket, "payload-limit")
      assert meta.chunks == 3
      assert :ok = Object.get(:gnat, bucket, meta.name, &send(self(), {:chunk, &1}))
      assert_received {:chunk, <<_::binary-size(2048)>>}
      assert_received {:chunk, <<_::binary-size(2048)>>}
      assert_received {:chunk, <<_::binary-size(904)>>}
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "respects the bucket's chunk limit" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket, max_chunk_size: 1024)
      data = :binary.copy("a", 2500)

      assert {:ok, meta} = put_binary(data, bucket, "small-chunks")
      assert meta.chunks == 3
      assert meta.size == byte_size(data)
      assert :ok = Object.get(:gnat, bucket, meta.name, &send(self(), {:chunk, &1}))
      assert_received {:chunk, <<_::binary-size(1024)>>}
      assert_received {:chunk, <<_::binary-size(1024)>>}
      assert_received {:chunk, <<_::binary-size(452)>>}
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "returns chunk publish errors without publishing metadata" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket, max_bucket_size: 1024)

      assert {:error, %{"code" => 503}} = put_binary(:binary.copy("a", 2048), bucket, "large")
      assert {:error, %{"code" => 404}} = Object.info(:gnat, bucket, "large")
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "returns metadata publish errors" do
      bucket = nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, bucket, max_chunk_size: 128)

      assert {:error, %{"code" => 400}} = put_binary("", bucket, "metadata-too-large")
      assert {:error, %{"code" => 404}} = Object.info(:gnat, bucket, "metadata-too-large")
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "rejects malformed and unrelated publish acknowledgements" do
      bucket = nuid()
      assert {:ok, %{config: config}} = Object.create_bucket(:gnat, bucket)
      assert {:ok, _} = Stream.update(:gnat, %{config | subjects: ["$O.#{bucket}.M.>"]})
      parent = self()

      responder =
        start_supervised!(
          {Task,
           fn ->
             {:ok, sid} = Gnat.sub(:gnat, self(), "$O.#{bucket}.C.>")
             send(parent, :responder_ready)

             for response <- [
                   "not json",
                   "{}",
                   ~s({"stream":"other","seq":1}),
                   ~s({"stream":"OBJ_#{bucket}","seq":0})
                 ] do
               receive do
                 {:msg, %{reply_to: reply}} -> Gnat.pub(:gnat, reply, response)
               end
             end

             Gnat.unsub(:gnat, sid)
           end}
        )

      assert_receive :responder_ready

      for _ <- 1..4 do
        assert {:error, :invalid_publish_ack} = put_binary("data", bucket, "invalid-ack")
        assert {:error, %{"code" => 404}} = Object.info(:gnat, bucket, "invalid-ack")
      end

      ref = Process.monitor(responder)
      assert_receive {:DOWN, ^ref, :process, ^responder, _}
      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "creates an object" do
      assert {:ok, %{config: _stream}} = Object.create_bucket(:gnat, "MY-STORE")

      expected_sha = @readme_path |> File.read!() |> then(&:crypto.hash(:sha256, &1))
      assert {:ok, object_meta} = put_filepath(@readme_path, "MY-STORE", "README.md")
      assert object_meta.name == "README.md"
      assert object_meta.bucket == "MY-STORE"
      assert object_meta.chunks == 1
      assert "SHA-256=" <> encoded = object_meta.digest
      assert Base.url_decode64!(encoded) == expected_sha

      assert :ok = Object.delete_bucket(:gnat, "MY-STORE")
    end

    test "overwriting a file" do
      bucket = nuid()
      assert {:ok, %{config: _stream}} = Object.create_bucket(:gnat, bucket)
      assert {:ok, _} = put_binary(:binary.copy("a", 20_000), bucket, "WAT")
      size_after_large = stream_byte_size(bucket)
      assert {:ok, _} = put_binary(:binary.copy("a", 1_000), bucket, "WAT")
      size_after_small = stream_byte_size(bucket)
      assert size_after_small < size_after_large
      assert {:ok, [meta]} = Object.list(:gnat, bucket)
      assert meta.name == "WAT"

      assert :ok = Object.delete_bucket(:gnat, bucket)
    end

    test "return an error if the object store doesn't exist" do
      assert {:error, err} = put_filepath(@readme_path, "I_DONT_EXIST", "foo")
      assert %{"code" => 404, "description" => "stream not found"} = err
    end
  end

  @tag :tmp_dir
  test "storing and retrieving larger files", %{tmp_dir: tmp_dir} do
    assert {:ok, path, sha} = generate_big_file(tmp_dir)
    bucket = nuid()
    assert {:ok, %{config: _stream}} = Object.create_bucket(:gnat, bucket)
    assert {:ok, meta} = put_filepath(path, bucket, "big")
    assert meta.chunks == 800
    assert meta.size == 800 * 128 * 1024
    assert "SHA-256=" <> encoded = meta.digest
    assert Base.url_decode64!(encoded) == sha

    Process.put(:buffer, "")

    Object.get(:gnat, bucket, "big", fn chunk ->
      Process.put(:buffer, Process.get(:buffer) <> chunk)
    end)

    file_contents = Process.get(:buffer)
    assert byte_size(file_contents) == meta.size
    assert :crypto.hash(:sha256, file_contents) == sha
    assert stream_byte_size(bucket) > 1024 * 1024

    assert :ok = Object.delete(:gnat, bucket, "big")
    assert stream_byte_size(bucket) < 1024
    :ok = Object.delete_bucket(:gnat, bucket)
  end

  @tag :tmp_dir
  test "control messages don't affect chunk count", %{tmp_dir: tmp_dir} do
    assert {:ok, path, _sha} = generate_big_file(tmp_dir)
    bucket = nuid()
    assert {:ok, %{config: _stream}} = Object.create_bucket(:gnat, bucket)
    assert {:ok, meta} = put_filepath(path, bucket, "test_chunks")

    Process.put(:chunk_count, 0)

    :ok =
      Object.get(:gnat, bucket, "test_chunks", fn _chunk ->
        Process.put(:chunk_count, Process.get(:chunk_count) + 1)
      end)

    chunk_count = Process.get(:chunk_count)

    assert chunk_count == meta.chunks

    :ok = Object.delete_bucket(:gnat, bucket)
  end

  describe "list_buckets/2" do
    test "list buckets when none exists" do
      assert {:ok, []} = Object.list_buckets(:gnat)
    end

    test "list buckets properly" do
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, "TEST_BUCKET_1")
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, "TEST_BUCKET_2")
      assert {:ok, ["TEST_BUCKET_1", "TEST_BUCKET_2"]} = Object.list_buckets(:gnat)
      :ok = Object.delete_bucket(:gnat, "TEST_BUCKET_1")
      :ok = Object.delete_bucket(:gnat, "TEST_BUCKET_2")
    end

    test "ignore streams that are not buckets" do
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, "TEST_BUCKET_1")

      stream = %Stream{
        name: "TEST_STREAM_1",
        subjects: ["TEST_STREAM_1.subject1", "TEST_STREAM_1.subject2"]
      }

      assert {:ok, _response} = Stream.create(:gnat, stream)
      assert {:ok, ["TEST_BUCKET_1"]} = Object.list_buckets(:gnat)
      :ok = Object.delete_bucket(:gnat, "TEST_BUCKET_1")
    end
  end

  # create a random 100MB binary file
  # re-use it on subsequent test runs if it already exists
  defp generate_big_file(tmp_dir) do
    filepath = Path.join(tmp_dir, "big_file.bin")
    sha = :crypto.hash_init(:sha256)
    {:ok, fh} = File.open(filepath, [:write])

    sha =
      Enum.reduce(1..800, sha, fn _, digest ->
        rand_chunk = :crypto.strong_rand_bytes(128) |> String.duplicate(1024)
        :ok = IO.binwrite(fh, rand_chunk)
        :crypto.hash_update(digest, rand_chunk)
      end)

    :ok = File.close(fh)
    {:ok, filepath, :crypto.hash_final(sha)}
  end

  defp put_filepath(path, bucket, name) do
    File.open!(path, [:read], &Object.put(:gnat, bucket, name, &1))
  end

  defp put_binary(binary, bucket, name) do
    {:ok, io} = StringIO.open(binary)

    try do
      Object.put(:gnat, bucket, name, io)
    after
      StringIO.close(io)
    end
  end

  defp stream_byte_size(bucket) do
    {:ok, %{state: state}} = Stream.info(:gnat, "OBJ_#{bucket}")
    state.bytes
  end

  defp replace_meta(meta) do
    topic = "$O.#{meta.bucket}.M.#{Base.url_encode64(meta.name)}"

    assert {:ok, %{body: body}} =
             Gnat.request(:gnat, topic, Jason.encode!(meta), headers: [{"Nats-Rollup", "sub"}])

    assert %{"seq" => _} = Jason.decode!(body)
  end

  defp assert_read_resources_released(bucket) do
    # The connection retains its shared request/reply subscription.
    assert {:ok, 1} = Gnat.active_subscriptions(:gnat)
    assert {:ok, %{consumers: consumers}} = Consumer.list(:gnat, "OBJ_#{bucket}")
    assert consumers in [nil, []]
  end

  defp create_limited_bucket(bucket) do
    assert {:ok, %{config: config}} =
             Stream.create(:gnat, %Stream{
               name: "OBJ_#{bucket}",
               subjects: ["$O.#{bucket}.C.>", "$O.#{bucket}.M.>"],
               discard: :new,
               allow_rollup_hdrs: true,
               max_consumers: 1
             })

    cleanup_bucket_on_exit(bucket)
    config
  end

  defp cleanup_bucket_on_exit(bucket) do
    on_exit(fn ->
      {:ok, conn} = Gnat.start_link()

      try do
        assert :ok = Object.delete_bucket(conn, bucket)
      after
        Gnat.stop(conn)
      end
    end)
  end
end
