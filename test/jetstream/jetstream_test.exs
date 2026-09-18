defmodule Gnat.JetstreamTest do
  use Gnat.Jetstream.ConnCase

  describe "ack_next" do
    @describetag with_gnat: :gnat

    setup do
      {:ok, _} = Gnat.sub(:gnat, self(), "ack.subject")

      %{message: %{gnat: :gnat, reply_to: "ack.subject"}}
    end

    test "ack_next/2 emits the plain acknowledgement", %{message: message} do
      assert :ok = Gnat.Jetstream.ack_next(message, "consumer.subject")

      assert_receive {:msg, %{body: "+NXT", topic: "ack.subject", reply_to: "consumer.subject"}}
    end

    test "ack_next/3 emits pull options", %{message: message} do
      assert :ok =
               Gnat.Jetstream.ack_next(message, "consumer.subject",
                 expires: 5_000_000_000,
                 idle_heartbeat: 2_500_000_000,
                 no_wait: true
               )

      assert_receive {:msg,
                      %{
                        body: "+NXT " <> payload,
                        topic: "ack.subject",
                        reply_to: "consumer.subject"
                      }}

      assert Jason.decode!(payload) == %{
               "batch" => 1,
               "expires" => 5_000_000_000,
               "idle_heartbeat" => 2_500_000_000,
               "no_wait" => true
             }
    end

    test "ack_next/3 rejects invalid options", %{message: message} do
      assert_raise ArgumentError, ~r/unknown keys \[:invalid\]/, fn ->
        Gnat.Jetstream.ack_next(message, "consumer.subject", invalid: true)
      end
    end

    test "both arities reject messages without a reply subject" do
      message = %{reply_to: nil}

      assert {:error, "Cannot ack message with no reply-to"} =
               Gnat.Jetstream.ack_next(message, "consumer.subject")

      assert {:error, "Cannot ack message with no reply-to"} =
               Gnat.Jetstream.ack_next(message, "consumer.subject", expires: 1)
    end
  end
end
