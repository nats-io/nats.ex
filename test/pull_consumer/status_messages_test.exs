defmodule Gnat.Jetstream.PullConsumer.StatusMessagesTest do
  use Gnat.Jetstream.ConnCase

  alias Gnat.Jetstream.API.{Consumer, Stream}

  defmodule ObservingConsumer do
    use Gnat.Jetstream.PullConsumer

    def start_link(opts) do
      Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      {test_pid, opts} = Keyword.pop!(opts, :test_pid)
      state = Keyword.merge([connection_name: :gnat], opts)

      {:ok, %{test_pid: test_pid}, state}
    end

    @impl true
    def handle_message(message, state) do
      send(state.test_pid, {:handle_message, message})
      {:ack, state}
    end

    @impl true
    def handle_status(message, state) do
      send(state.test_pid, {:handle_status, message})
      {:ok, state}
    end
  end

  defmodule SilentConsumer do
    use Gnat.Jetstream.PullConsumer

    def start_link(opts) do
      Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      {test_pid, opts} = Keyword.pop!(opts, :test_pid)

      {:ok, test_pid, Keyword.merge([connection_name: :gnat], opts)}
    end

    @impl true
    def handle_message(message, test_pid) do
      send(test_pid, {:handle_message, message})
      {:ack, test_pid}
    end
  end

  describe "JetStream status messages" do
    @describetag with_gnat: :gnat

    setup do
      stream_name = "STATUS_TEST_STREAM"
      consumer_name = "STATUS_TEST_CONSUMER"

      stream = %Stream{name: stream_name, subjects: ["status.test"]}
      {:ok, _} = Stream.create(:gnat, stream)

      consumer = %Consumer{stream_name: stream_name, durable_name: consumer_name}
      {:ok, _} = Consumer.create(:gnat, consumer)

      on_exit(fn ->
        {:ok, pid} = Gnat.start_link()
        :ok = Stream.delete(pid, stream_name)
        Gnat.stop(pid)
      end)

      %{stream_name: stream_name, consumer_name: consumer_name}
    end

    test "are not forwarded to handle_message when no handle_status is defined",
         %{stream_name: stream_name, consumer_name: consumer_name} do
      pid =
        start_supervised!(
          {SilentConsumer,
           test_pid: self(), stream_name: stream_name, consumer_name: consumer_name}
        )

      send(pid, {:msg, %{status: "409", description: "Leadership Change", body: "", gnat: :gnat}})
      send(pid, {:msg, %{status: "100", body: "", gnat: :gnat}})

      refute_receive {:handle_message, _}, 100
    end

    test "are forwarded to handle_status when the callback is defined",
         %{stream_name: stream_name, consumer_name: consumer_name} do
      pid =
        start_supervised!(
          {ObservingConsumer,
           test_pid: self(), stream_name: stream_name, consumer_name: consumer_name}
        )

      send(pid, {:msg, %{status: "409", description: "Leadership Change", body: "", gnat: :gnat}})

      assert_receive {:handle_status, %{status: "409", description: "Leadership Change"}}
      refute_receive {:handle_message, _}, 100
    end

    test "single-message mode issues a new pull after a 409",
         %{stream_name: stream_name, consumer_name: consumer_name} do
      # Subscribe to the next-message subject to observe outbound pulls.
      {:ok, _sid} =
        Gnat.sub(:gnat, self(), "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.#{consumer_name}")

      pid =
        start_supervised!(
          {SilentConsumer,
           test_pid: self(), stream_name: stream_name, consumer_name: consumer_name}
        )

      # Drain the initial pull issued on connect.
      assert_receive {:msg, %{topic: "$JS.API.CONSUMER.MSG.NEXT." <> _}}, 1_000

      send(pid, {:msg, %{status: "409", description: "Leadership Change", body: "", gnat: :gnat}})

      # A new pull must be issued or the consumer is stuck.
      assert_receive {:msg, %{topic: "$JS.API.CONSUMER.MSG.NEXT." <> _}}, 1_000
    end
  end

  describe "batch-mode JetStream status messages" do
    @describetag with_gnat: :gnat

    setup do
      stream_name = "STATUS_BATCH_STREAM"
      subject = "status.batch"

      stream = %Stream{name: stream_name, subjects: [subject]}
      {:ok, _} = Stream.create(:gnat, stream)

      on_exit(fn ->
        {:ok, pid} = Gnat.start_link()
        :ok = Stream.delete(pid, stream_name)
        Gnat.stop(pid)
      end)

      %{stream_name: stream_name, subject: subject}
    end

    defmodule BatchObservingConsumer do
      use Gnat.Jetstream.PullConsumer

      def start_link(opts) do
        Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)
      end

      @impl true
      def init(opts) do
        {test_pid, opts} = Keyword.pop!(opts, :test_pid)
        state = Keyword.merge([connection_name: :gnat], opts)
        {:ok, %{test_pid: test_pid}, state}
      end

      @impl true
      def handle_message(message, state) do
        send(state.test_pid, {:handled, message.body})
        {:ack, state}
      end

      @impl true
      def handle_status(message, state) do
        send(state.test_pid, {:status, message})
        {:ok, state}
      end
    end

    test "batch mode issues a new pull after 409 with empty buffer",
         %{stream_name: stream_name} do
      consumer = %Gnat.Jetstream.API.Consumer{
        stream_name: stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      # Subscribe to the next-message subject before starting the consumer so
      # we see the pull the consumer issues at connect time and any re-pulls.
      {:ok, _sid} = Gnat.sub(:gnat, self(), "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.>")

      pid =
        start_supervised!(
          {BatchObservingConsumer, consumer: consumer, batch_size: 5, test_pid: self()}
        )

      # Drain pulls issued during connect (at minimum the initial catch-up fetch).
      # Absorb any that arrive within a short window so the assert below only
      # fires on the post-409 re-pull.
      drain_pulls(200)

      send(pid, {:msg, %{status: "409", description: "Leadership Change", body: "", gnat: :gnat}})

      # A new pull must be issued or the consumer is stuck.
      assert_receive {:msg, %{topic: "$JS.API.CONSUMER.MSG.NEXT." <> _}}, 1_000
      # And handle_status should have observed the 409.
      assert_receive {:status, %{status: "409", description: "Leadership Change"}}, 1_000
    end

    test "batch mode processes partial buffer and re-pulls on 409",
         %{stream_name: stream_name, subject: subject} do
      consumer = %Gnat.Jetstream.API.Consumer{
        stream_name: stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      {:ok, _sid} = Gnat.sub(:gnat, self(), "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.>")

      # Publish a single message so the consumer buffers 1 of a 5-message batch.
      :ok = Gnat.pub(:gnat, subject, "buffered-1")

      pid =
        start_supervised!(
          {BatchObservingConsumer, consumer: consumer, batch_size: 5, test_pid: self()}
        )

      # The buffered message should arrive at the server-issued 404 terminator
      # and be processed. Once that's settled, drain any further pulls.
      assert_receive {:handled, "buffered-1"}, 5_000
      drain_pulls(200)

      # Now stuff a new message into the buffer via direct send and inject a 409.
      fake_msg = %{
        topic: subject,
        body: "partial-1",
        reply_to: "$JS.ACK.#{stream_name}.FAKE.1.1.1.0.0",
        gnat: :gnat
      }

      send(pid, {:msg, fake_msg})

      # A 409 should cause the partial buffer to be processed (handle_message
      # called for "partial-1") and a fresh pull to be issued.
      send(pid, {:msg, %{status: "409", description: "Leadership Change", body: "", gnat: :gnat}})

      assert_receive {:handled, "partial-1"}, 1_000
      assert_receive {:msg, %{topic: "$JS.API.CONSUMER.MSG.NEXT." <> _}}, 1_000
      assert_receive {:status, %{status: "409", description: "Leadership Change"}}, 1_000
    end

    test "batch mode treats 100 heartbeat as a no-op (no re-pull)",
         %{stream_name: stream_name} do
      consumer = %Gnat.Jetstream.API.Consumer{
        stream_name: stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      {:ok, _sid} = Gnat.sub(:gnat, self(), "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.>")

      pid =
        start_supervised!(
          {BatchObservingConsumer, consumer: consumer, batch_size: 5, test_pid: self()}
        )

      drain_pulls(200)

      # 100 is a keep-alive on the existing pull — must NOT cause a re-pull.
      send(pid, {:msg, %{status: "100", body: "", gnat: :gnat}})

      refute_receive {:msg, %{topic: "$JS.API.CONSUMER.MSG.NEXT." <> _}}, 200
      # But handle_status should still be invoked so users can observe heartbeats.
      assert_receive {:status, %{status: "100"}}, 500
    end
  end

  defp drain_pulls(timeout) do
    receive do
      {:msg, %{topic: "$JS.API.CONSUMER.MSG.NEXT." <> _}} -> drain_pulls(timeout)
    after
      timeout -> :ok
    end
  end
end
