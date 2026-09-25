defmodule Gnat.Jetstream.API.KV.EntryTest do
  use ExUnit.Case, async: true

  alias Gnat.Jetstream.API.KV.Entry

  @bucket "my_bucket"
  @reply_to "$JS.ACK.KV_my_bucket.consumer.1.42.7.1750928948439739269.3"

  describe "from_message/2" do
    test "parses a put message with no headers" do
      message = %{topic: "$KV.my_bucket.some.key", body: "hello", reply_to: @reply_to}

      assert {:ok, entry} = Entry.from_message(message, @bucket)

      assert %Entry{
               bucket: "my_bucket",
               key: "some.key",
               value: "hello",
               operation: :put,
               revision: 42,
               delta: 3
             } = entry

      assert %DateTime{} = entry.created
    end

    test "parses a put message when headers are present but not KV-operation" do
      message = %{
        topic: "$KV.my_bucket.foo",
        body: "bar",
        headers: [{"some-other", "value"}]
      }

      assert {:ok, %Entry{operation: :put, key: "foo", value: "bar"}} =
               Entry.from_message(message, @bucket)
    end

    test "parses a DEL message as :delete" do
      message = %{
        topic: "$KV.my_bucket.foo",
        body: "",
        headers: [{"kv-operation", "DEL"}]
      }

      assert {:ok, %Entry{operation: :delete, key: "foo", value: ""}} =
               Entry.from_message(message, @bucket)
    end

    test "parses a PURGE message as :purge" do
      message = %{
        topic: "$KV.my_bucket.foo",
        body: "",
        headers: [{"kv-operation", "PURGE"}]
      }

      assert {:ok, %Entry{operation: :purge, key: "foo"}} =
               Entry.from_message(message, @bucket)
    end

    test "treats a nats-marker-reason tombstone as a removal" do
      message = %{
        topic: "$KV.my_bucket.foo",
        body: "",
        headers: [{"nats-marker-reason", "MaxAge"}]
      }

      assert {:ok, %Entry{operation: :purge, key: "foo"}} =
               Entry.from_message(message, @bucket)
    end

    test "recovers keys that include dots" do
      message = %{topic: "$KV.my_bucket.a.b.c", body: "v"}

      assert {:ok, %Entry{key: "a.b.c"}} = Entry.from_message(message, @bucket)
    end

    test "leaves revision/created/delta nil when no reply_to is present" do
      message = %{topic: "$KV.my_bucket.foo", body: "bar"}

      assert {:ok, %Entry{revision: nil, created: nil, delta: nil}} =
               Entry.from_message(message, @bucket)
    end

    test "returns :ignore when the subject does not belong to the bucket" do
      message = %{topic: "$KV.other_bucket.foo", body: "bar"}

      assert :ignore = Entry.from_message(message, @bucket)
    end

    test "returns :ignore for a 409 leadership-change status message" do
      message = %{
        topic: "_INBOX.foo",
        body: "",
        status: "409",
        description: "Leadership Change"
      }

      assert :ignore = Entry.from_message(message, @bucket)
    end

    test "returns :ignore for a 100 idle heartbeat with no description" do
      message = %{topic: "_INBOX.foo", body: "", status: "100"}

      assert :ignore = Entry.from_message(message, @bucket)
    end
  end

  describe "operation/1" do
    test "classifies kv-operation headers" do
      assert :delete == Entry.operation([{"kv-operation", "DEL"}])
      assert :purge == Entry.operation([{"kv-operation", "PURGE"}])
      assert :put == Entry.operation([{"kv-operation", "SOMETHING_ELSE"}])
    end

    test "classifies server-generated marker reasons like the official clients" do
      assert :purge == Entry.operation([{"nats-marker-reason", "MaxAge"}])
      assert :purge == Entry.operation([{"nats-marker-reason", "Purge"}])
      assert :delete == Entry.operation([{"nats-marker-reason", "Remove"}])
      assert :put == Entry.operation([{"nats-marker-reason", "SomethingNew"}])
    end

    test "kv-operation takes precedence over a marker reason" do
      headers = [{"nats-marker-reason", "Remove"}, {"kv-operation", "PURGE"}]
      assert :purge == Entry.operation(headers)

      headers = [{"kv-operation", "CUSTOM"}, {"nats-marker-reason", "Remove"}]
      assert :put == Entry.operation(headers)
    end

    test "falls back to the marker reason when kv-operation is empty" do
      assert :delete == Entry.operation([{"kv-operation", ""}, {"nats-marker-reason", "Remove"}])
    end

    test "treats unrelated or missing headers as a put" do
      assert :put == Entry.operation([{"nats-msg-id", "abc"}])
      assert :put == Entry.operation([])
      assert :put == Entry.operation(nil)
    end
  end
end
