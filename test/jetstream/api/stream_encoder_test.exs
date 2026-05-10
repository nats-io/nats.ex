defmodule Gnat.Jetstream.API.StreamEncoderTest do
  # Pure unit tests for the Stream Jason.Encoder impl. These cover the
  # serialization contract that issue #222 broke against nats-server 2.14:
  # the server uses strict JSON parsing (DisallowUnknownFields), so the client
  # must only send fields the user explicitly set.
  use ExUnit.Case, async: true

  alias Gnat.Jetstream.API.Stream

  defp encoded_keys(stream), do: stream |> Jason.encode!() |> Jason.decode!() |> Map.keys()
  defp decoded(stream), do: stream |> Jason.encode!() |> Jason.decode!()

  describe "Jason.Encoder" do
    test "omits nil-valued fields so unknown-field-strict servers accept the request" do
      stream = %Stream{name: "ORDERS", subjects: ["orders.*"]}
      keys = encoded_keys(stream)

      assert Enum.sort(keys) == ["name", "subjects"]
    end

    test "does not send :template_owner unless the user set it (regression for #222)" do
      stream = %Stream{name: "ORDERS", subjects: ["orders.*"]}
      refute "template_owner" in encoded_keys(stream)
    end

    test "still sends :template_owner when the user explicitly set it" do
      stream = %Stream{name: "ORDERS", subjects: ["orders.*"], template_owner: "tmpl-1"}
      assert decoded(stream)["template_owner"] == "tmpl-1"
    end

    test "preserves boolean false (only nil is dropped)" do
      stream = %Stream{name: "X", subjects: ["x"], deny_delete: false, allow_direct: false}
      payload = decoded(stream)

      assert payload["deny_delete"] == false
      assert payload["allow_direct"] == false
    end

    test "preserves zero and -1 (only nil is dropped)" do
      stream = %Stream{name: "X", subjects: ["x"], max_age: 0, max_bytes: -1}
      payload = decoded(stream)

      assert payload["max_age"] == 0
      assert payload["max_bytes"] == -1
    end

    test "encodes atom enum values as strings the server understands" do
      stream = %Stream{
        name: "X",
        subjects: ["x"],
        retention: :workqueue,
        storage: :memory,
        discard: :new
      }

      payload = decoded(stream)

      assert payload["retention"] == "workqueue"
      assert payload["storage"] == "memory"
      assert payload["discard"] == "new"
    end

    test "drops :domain (it's part of the API subject, not the request body)" do
      stream = %Stream{name: "X", subjects: ["x"], domain: "leaf-1"}
      refute "domain" in encoded_keys(stream)
    end

    test "passes nested maps (placement) through unchanged" do
      placement = %{cluster: "us-east", tags: ["ssd", "fast"]}
      stream = %Stream{name: "X", subjects: ["x"], placement: placement}

      assert decoded(stream)["placement"] == %{
               "cluster" => "us-east",
               "tags" => ["ssd", "fast"]
             }
    end

    test "round-trips a fully-populated stream without dropping fields" do
      stream = %Stream{
        name: "FULL",
        subjects: ["full.*"],
        description: "every field set",
        retention: :limits,
        storage: :file,
        discard: :old,
        max_age: 1_000,
        max_bytes: 10_000,
        max_consumers: 5,
        max_msg_size: 1024,
        max_msgs: 100,
        max_msgs_per_subject: 10,
        num_replicas: 3,
        duplicate_window: 60_000_000_000,
        no_ack: false,
        sealed: false,
        deny_delete: false,
        deny_purge: false,
        allow_direct: true,
        allow_msg_ttl: true,
        allow_rollup_hdrs: true,
        mirror_direct: false,
        discard_new_per_subject: false,
        subject_delete_marker_ttl: 0,
        compression: "s2",
        template_owner: "tmpl"
      }

      payload = decoded(stream)

      # name + subjects + 24 explicitly-set optional fields = 26
      assert map_size(payload) == 26
      assert payload["compression"] == "s2"
      assert payload["template_owner"] == "tmpl"
    end
  end
end
