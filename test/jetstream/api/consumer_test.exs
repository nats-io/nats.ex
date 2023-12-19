defmodule Gnat.Jetstream.API.ConsumerTest do
  use Gnat.Jetstream.ConnCase
  alias Gnat.Jetstream.API.{Consumer, Stream}

  @moduletag with_gnat: :gnat

  test "listing, creating, and deleting consumers" do
    stream = %Stream{name: "STREAM1", subjects: ["STREAM1"]}
    {:ok, _response} = Stream.create(:gnat, stream)

    assert {:ok, consumers} = Consumer.list(:gnat, "STREAM1")

    assert consumers == %{
             total: 0,
             offset: 0,
             limit: 1024,
             consumers: []
           }

    consumer = %Consumer{stream_name: "STREAM1", durable_name: "STREAM1"}
    assert {:ok, consumer_response} = Consumer.create(:gnat, consumer)

    assert consumer_response.ack_floor == %{
             consumer_seq: 0,
             stream_seq: 0
           }

    assert consumer_response.delivered == %{
             consumer_seq: 0,
             stream_seq: 0
           }

    assert %DateTime{} = consumer_response.created

    assert consumer_response.config == %{
             ack_policy: :explicit,
             ack_wait: 30_000_000_000,
             deliver_policy: :all,
             deliver_subject: nil,
             durable_name: "STREAM1",
             filter_subject: nil,
             opt_start_seq: nil,
             opt_start_time: nil,
             replay_policy: :instant,
             backoff: nil,
             deliver_group: nil,
             description: nil,
             flow_control: nil,
             headers_only: nil,
             idle_heartbeat: nil,
             inactive_threshold: nil,
             max_ack_pending: 20000,
             max_batch: nil,
             max_deliver: -1,
             max_expires: nil,
             max_waiting: 512,
             rate_limit_bps: nil,
             sample_freq: nil
           }

    assert consumer_response.num_pending == 0
    assert consumer_response.num_redelivered == 0

    assert {:ok, consumers} = Consumer.list(:gnat, "STREAM1")

    assert consumers == %{
             total: 1,
             offset: 0,
             limit: 1024,
             consumers: ["STREAM1"]
           }

    assert :ok = Consumer.delete(:gnat, "STREAM1", "STREAM1")
    assert {:ok, consumers} = Consumer.list(:gnat, "STREAM1")

    assert consumers == %{
             total: 0,
             offset: 0,
             limit: 1024,
             consumers: []
           }
  end

  test "failed creates" do
    consumer = %Consumer{durable_name: "STREAM2", stream_name: "STREAM2"}

    assert {:error, %{"code" => 404, "description" => "stream not found"}} =
             Consumer.create(:gnat, consumer)
  end

  test "failed deletes" do
    assert {:error, %{"code" => 404, "description" => "stream not found"}} =
             Consumer.delete(:gnat, "STREAM3", "STREAM3")
  end

  test "getting consumer info" do
    stream = %Stream{name: "STREAM4", subjects: ["STREAM4"]}
    {:ok, _response} = Stream.create(:gnat, stream)

    consumer = %Consumer{
      stream_name: "STREAM4",
      durable_name: "STREAM4",
      deliver_subject: "consumer.STREAM4"
    }

    assert {:ok, _consumer_response} = Consumer.create(:gnat, consumer)

    assert {:ok, consumer_response} = Consumer.info(:gnat, "STREAM4", "STREAM4")

    assert consumer_response.ack_floor == %{
             consumer_seq: 0,
             stream_seq: 0
           }

    assert consumer_response.delivered == %{
             consumer_seq: 0,
             stream_seq: 0
           }

    assert %DateTime{} = consumer_response.created

    assert consumer_response.config == %{
             ack_policy: :explicit,
             ack_wait: 30_000_000_000,
             deliver_policy: :all,
             deliver_subject: "consumer.STREAM4",
             durable_name: "STREAM4",
             filter_subject: nil,
             opt_start_seq: nil,
             opt_start_time: nil,
             replay_policy: :instant,
             backoff: nil,
             deliver_group: nil,
             description: nil,
             flow_control: nil,
             headers_only: nil,
             idle_heartbeat: nil,
             inactive_threshold: nil,
             max_ack_pending: 20000,
             max_batch: nil,
             max_deliver: -1,
             max_expires: nil,
             max_waiting: nil,
             rate_limit_bps: nil,
             sample_freq: nil
           }

    assert consumer_response.num_pending == 0
    assert consumer_response.num_redelivered == 0

    assert :ok = Consumer.delete(:gnat, "STREAM4", "STREAM4")
    assert :ok = Stream.delete(:gnat, "STREAM4")
  end

  test "validating stream and consumer names" do
    assert {:error, reason} =
             Consumer.create(:gnat, %Consumer{stream_name: "test.periods", durable_name: "foo"})

    assert reason == "invalid stream_name: cannot contain '.', '>', '*', spaces or tabs"

    assert {:error, reason} =
             Consumer.create(:gnat, %Consumer{stream_name: nil, durable_name: "foo"})

    assert reason == "must have a :stream_name set"

    assert {:error, reason} =
             Consumer.create(:gnat, %Consumer{stream_name: :foo, durable_name: "foo"})

    assert reason == "stream_name must be a string"

    assert {:error, reason} =
             Consumer.create(:gnat, %Consumer{stream_name: "TEST_STREAM", durable_name: "foo.bar"})

    assert reason == "invalid durable_name: cannot contain '.', '>', '*', spaces or tabs"

    assert {:error, reason} =
             Consumer.create(:gnat, %Consumer{stream_name: "TEST_STREAM", durable_name: :ohai})

    assert reason == "durable_name must be a string"
  end

  describe "request_next_message/5" do
    setup do
      stream_name = "REQUEST_MESSAGE_TEST_STREAM"
      subject = "request_test_subject"
      consumer_name = "REQUEST_MESSAGE_TEST_CONSUMER"

      Stream.delete(:gnat, stream_name)

      stream = %Stream{name: stream_name, subjects: [subject]}
      {:ok, _response} = Stream.create(:gnat, stream)

      consumer = %Consumer{stream_name: stream_name, durable_name: consumer_name}
      assert {:ok, _response} = Consumer.create(:gnat, consumer)

      reply_subject = "reply"

      Gnat.sub(:gnat, self(), reply_subject)

      %{
        stream_name: stream_name,
        subject: subject,
        consumer_name: consumer_name,
        reply_subject: reply_subject
      }
    end

    test "requests a single message with default options", %{
      stream_name: stream_name,
      subject: subject,
      consumer_name: consumer_name,
      reply_subject: reply_subject
    } do
      Consumer.request_next_message(:gnat, stream_name, consumer_name, reply_subject)

      Gnat.pub(:gnat, subject, "message 1")

      assert_receive {:msg, %{body: "message 1", topic: ^subject}}

      Gnat.pub(:gnat, subject, "message 2")

      refute_receive {:msg, %{body: "message 2"}}
    end

    test "requests batch messages", %{
      stream_name: stream_name,
      subject: subject,
      consumer_name: consumer_name,
      reply_subject: reply_subject
    } do
      Consumer.request_next_message(:gnat, stream_name, consumer_name, reply_subject, nil,
        batch: 10
      )

      Gnat.pub(:gnat, subject, "message 1")

      assert_receive {:msg, %{body: "message 1", topic: ^subject}}

      for i <- 2..10, do: Gnat.pub(:gnat, subject, "message #{i}")

      for i <- 2..10 do
        expected_body = "message #{i}"

        assert_receive {:msg, %{body: ^expected_body, topic: ^subject}}
      end

      Gnat.pub(:gnat, subject, "message 11")

      refute_receive {:msg, %{body: "message 11"}}
    end

    test "doesn't wait for messages when `no_wait` option is set to true", %{
      stream_name: stream_name,
      subject: subject,
      consumer_name: consumer_name,
      reply_subject: reply_subject
    } do
      Consumer.request_next_message(:gnat, stream_name, consumer_name, reply_subject, nil,
        no_wait: true
      )

      assert_receive {:msg, %{body: "", topic: ^reply_subject}}

      Gnat.pub(:gnat, subject, "message 1")

      refute_receive {:msg, %{body: "message 1"}}
    end

    test "doesn't wait for messages to complete the batch size with `no_wait`", %{
      stream_name: stream_name,
      subject: subject,
      consumer_name: consumer_name,
      reply_subject: reply_subject
    } do
      for i <- 1..9 do
        # Using `request` to make sure messages get in the stream before we move on to consume them
        assert {:ok, _} = Gnat.request(:gnat, subject, "message #{i}")
      end

      Consumer.request_next_message(:gnat, stream_name, consumer_name, reply_subject, nil,
        batch: 10,
        no_wait: true
      )

      for i <- 1..9 do
        expected_body = "message #{i}"

        assert_receive {:msg, %{body: ^expected_body, topic: ^subject}}, 2_000
      end

      assert_receive {:msg, %{body: "", topic: ^reply_subject}}, 2_000

      Gnat.pub(:gnat, subject, "message 10")

      refute_receive {:msg, %{body: "message 10"}}
    end
  end
end
