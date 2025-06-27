defmodule Gnat.Jetstream.MessageTest do
  use ExUnit.Case, async: true

  test "message metadata without domain" do
    assert {:ok,
            %Gnat.Jetstream.API.Message.Metadata{
              stream_seq: 1_428_472,
              consumer_seq: 20,
              num_pending: 3283,
              num_delivered: 4,
              timestamp: ~U[2025-06-26 09:09:08.439739Z],
              stream: "STREAM",
              consumer: "consumer",
              domain: nil
            }} =
             Gnat.Jetstream.API.Message.metadata(%{
               reply_to: "$JS.ACK.STREAM.consumer.4.1428472.20.1750928948439739269.3283"
             })
  end

  test "message metadata with domain" do
    assert {:ok,
            %Gnat.Jetstream.API.Message.Metadata{
              stream_seq: 1_428_472,
              consumer_seq: 20,
              num_pending: 3283,
              num_delivered: 4,
              timestamp: ~U[2025-06-26 09:09:08.439739Z],
              stream: "STREAM",
              consumer: "consumer",
              domain: "test"
            }} =
             Gnat.Jetstream.API.Message.metadata(%{
               reply_to:
                 "$JS.ACK.test.$G.STREAM.consumer.4.1428472.20.1750928948439739269.3283.279619330"
             })
  end
end
