defmodule Gnat.Jetstream.PullConsumer.BatchTest do
  use Gnat.Jetstream.ConnCase

  alias Gnat.Jetstream.API.{Consumer, Stream}

  @stream_name "BATCH_TEST_STREAM"
  @subject "batch_test.*"

  defmodule BatchPullConsumer do
    use Gnat.Jetstream.PullConsumer

    def start_link(opts) do
      Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      batch_size = Keyword.fetch!(opts, :batch_size)

      connection_opts =
        [connection_name: :gnat, batch_size: batch_size, request_expires: 500_000_000]
        |> maybe_put(:consumer, opts)
        |> maybe_put(:stream_name, opts)
        |> maybe_put(:consumer_name, opts)
        |> maybe_put(:connection_retry_timeout, opts)
        |> maybe_put(:connection_retries, opts)

      state = %{
        test_pid: Keyword.fetch!(opts, :test_pid),
        messages: [],
        call_count: 0
      }

      {:ok, state, connection_opts}
    end

    defp maybe_put(conn_opts, key, opts) do
      case Keyword.fetch(opts, key) do
        {:ok, value} -> Keyword.put(conn_opts, key, value)
        :error -> conn_opts
      end
    end

    @impl true
    def handle_connected(consumer_info, state) do
      send(state.test_pid, {:connected, consumer_info})
      {:ok, state}
    end

    @impl true
    def handle_message(message, state) do
      call_count = state.call_count + 1
      messages = [message.body | state.messages]
      send(state.test_pid, {:handled, call_count, message.body})
      {:ack, %{state | messages: messages, call_count: call_count}}
    end
  end

  # A consumer whose handle_message returns :nack or :term based on message body
  defmodule NonAckBatchConsumer do
    use Gnat.Jetstream.PullConsumer

    def start_link(opts) do
      Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      consumer = Keyword.fetch!(opts, :consumer)
      batch_size = Keyword.fetch!(opts, :batch_size)

      connection_opts = [
        connection_name: :gnat,
        consumer: consumer,
        batch_size: batch_size
      ]

      state = %{test_pid: Keyword.fetch!(opts, :test_pid)}
      {:ok, state, connection_opts}
    end

    @impl true
    def handle_message(%{body: "nack_me"}, state) do
      send(state.test_pid, {:returned_nack})
      {:nack, state}
    end

    def handle_message(%{body: "term_me"}, state) do
      send(state.test_pid, {:returned_term})
      {:term, state}
    end

    def handle_message(message, state) do
      send(state.test_pid, {:handled, message.body})
      {:ack, state}
    end
  end

  describe "batch mode" do
    @describetag with_gnat: :gnat

    setup do
      stream = %Stream{name: @stream_name, subjects: [@subject]}
      {:ok, _} = Stream.create(:gnat, stream)

      on_exit(fn ->
        {:ok, pid} = Gnat.start_link()
        Stream.delete(pid, @stream_name)
        Gnat.stop(pid)
      end)

      :ok
    end

    test "processes a full batch of messages" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      # Publish exactly batch_size messages before starting consumer
      for i <- 1..3 do
        :ok = Gnat.pub(:gnat, "batch_test.full", "msg-#{i}")
      end

      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 3, test_pid: self()})

      assert_receive {:connected, _consumer_info}

      # All 3 messages should be delivered individually to handle_message
      assert_receive {:handled, 1, "msg-1"}
      assert_receive {:handled, 2, "msg-2"}
      assert_receive {:handled, 3, "msg-3"}
    end

    test "processes partial batch when fewer messages than batch_size" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      # Only 2 messages with batch_size of 5
      :ok = Gnat.pub(:gnat, "batch_test.partial", "partial-1")
      :ok = Gnat.pub(:gnat, "batch_test.partial", "partial-2")

      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 5, test_pid: self()})

      assert_receive {:connected, _}

      # Partial batch should still be processed when terminal signal arrives
      assert_receive {:handled, 1, "partial-1"}, 3_000
      assert_receive {:handled, 2, "partial-2"}, 3_000
    end

    test "empty stream does not hang the consumer" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      pid =
        start_supervised!(
          {BatchPullConsumer, consumer: consumer, batch_size: 5, test_pid: self()}
        )

      assert_receive {:connected, consumer_info}
      assert consumer_info.num_pending == 0

      # Consumer should not hang — it should be alive and responsive
      assert Process.alive?(pid)

      # Now publish a message — consumer should still pick it up
      :ok = Gnat.pub(:gnat, "batch_test.empty", "late-arrival")

      # In batch mode with batch_size 5, a single message will arrive as a
      # partial batch (terminal signal triggers processing of the 1-message buffer)
      assert_receive {:handled, 1, "late-arrival"}, 10_000
    end

    test "continues processing after multiple batches" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      # Publish 6 messages with batch_size 3 — should produce 2 full batches
      for i <- 1..6 do
        :ok = Gnat.pub(:gnat, "batch_test.multi", "multi-#{i}")
      end

      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 3, test_pid: self()})

      assert_receive {:connected, _}

      # All 6 messages processed across 2 batches
      for i <- 1..6 do
        assert_receive {:handled, ^i, _body}, 5_000
      end
    end

    test "handles messages arriving after initial catch-up" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      # Start with some messages to catch up on
      for i <- 1..3 do
        :ok = Gnat.pub(:gnat, "batch_test.live", "catchup-#{i}")
      end

      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 3, test_pid: self()})

      assert_receive {:connected, _}

      # Wait for catch-up batch
      for i <- 1..3 do
        assert_receive {:handled, ^i, _body}, 5_000
      end

      # Now publish new messages — consumer should transition to tailing mode
      # and still pick these up
      :ok = Gnat.pub(:gnat, "batch_test.live", "live-1")
      :ok = Gnat.pub(:gnat, "batch_test.live", "live-2")

      assert_receive {:handled, 4, "live-1"}, 10_000
      assert_receive {:handled, 5, "live-2"}, 10_000
    end

    test "nack and term returns are treated as ack with warning logged" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      :ok = Gnat.pub(:gnat, "batch_test.nonack", "normal")
      :ok = Gnat.pub(:gnat, "batch_test.nonack", "nack_me")
      :ok = Gnat.pub(:gnat, "batch_test.nonack", "term_me")

      import ExUnit.CaptureLog

      log =
        capture_log(fn ->
          start_supervised!(
            {NonAckBatchConsumer, consumer: consumer, batch_size: 3, test_pid: self()}
          )

          assert_receive {:handled, "normal"}, 5_000
          assert_receive {:returned_nack}, 5_000
          assert_receive {:returned_term}, 5_000

          # Give time for the log to flush
          Process.sleep(100)
        end)

      # The warning should mention that batch mode doesn't support nack/term
      assert log =~ "batch mode does not support"
    end

    test "consumer does not get stuck after processing batch" do
      # This test verifies the critical flow: after a batch is processed and acked,
      # the consumer must issue another fetch request. If it doesn't, messages
      # published after the batch will never arrive.
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 2, test_pid: self()})

      assert_receive {:connected, _}

      # First batch
      :ok = Gnat.pub(:gnat, "batch_test.stuck", "a")
      :ok = Gnat.pub(:gnat, "batch_test.stuck", "b")

      assert_receive {:handled, 1, "a"}, 10_000
      assert_receive {:handled, 2, "b"}, 10_000

      # Wait a moment, then send another batch — consumer must not be stuck
      Process.sleep(200)

      :ok = Gnat.pub(:gnat, "batch_test.stuck", "c")
      :ok = Gnat.pub(:gnat, "batch_test.stuck", "d")

      assert_receive {:handled, 3, "c"}, 10_000
      assert_receive {:handled, 4, "d"}, 10_000

      # Third batch to confirm sustained flow
      Process.sleep(200)

      :ok = Gnat.pub(:gnat, "batch_test.stuck", "e")

      assert_receive {:handled, 5, "e"}, 10_000
    end

    test "batch_size 1 uses single-message mode (sends +NXT on ack)" do
      consumer_name = "BATCH_COMPAT_CONSUMER"

      consumer = %Consumer{
        stream_name: @stream_name,
        durable_name: consumer_name,
        inactive_threshold: 30_000_000_000,
        ack_policy: :explicit
      }

      # Subscribe to the ack subject to verify +NXT is sent (single-message pipeline)
      # rather than an empty body (batch-mode ack).
      {:ok, _} = Gnat.sub(:gnat, self(), "$JS.ACK.#{@stream_name}.#{consumer_name}.>")

      :ok = Gnat.pub(:gnat, "batch_test.compat", "compat-1")

      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 1, test_pid: self()})

      assert_receive {:handled, 1, "compat-1"}, 5_000
      assert_receive {:msg, %{body: "+NXT"}}, 5_000
    end

    test "batch mode with batch_size 2 and odd number of messages" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      # 5 messages with batch_size 2: batches of [2, 2, 1(partial)]
      for i <- 1..5 do
        :ok = Gnat.pub(:gnat, "batch_test.odd", "odd-#{i}")
      end

      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 2, test_pid: self()})

      assert_receive {:connected, _}

      for i <- 1..5 do
        assert_receive {:handled, ^i, _body}, 5_000
      end
    end

    test "large batch processes many messages correctly" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      msg_count = 50

      for i <- 1..msg_count do
        :ok = Gnat.pub(:gnat, "batch_test.large", "large-#{i}")
      end

      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 10, test_pid: self()})

      assert_receive {:connected, _}

      # All 50 messages should be delivered (5 batches of 10)
      for i <- 1..msg_count do
        assert_receive {:handled, ^i, _body}, 10_000
      end
    end

    test "can be closed cleanly during batch mode" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      pid =
        start_supervised!(
          {BatchPullConsumer, consumer: consumer, batch_size: 5, test_pid: self()}
        )

      assert_receive {:connected, _}

      ref = Process.monitor(pid)
      assert :ok = Gnat.Jetstream.PullConsumer.close(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    end

    test "handle_connected receives consumer info in batch mode" do
      # Publish messages before consumer starts, so num_pending > 0
      for i <- 1..3 do
        {:ok, _} = Gnat.request(:gnat, "batch_test.connected", "pre-#{i}")
      end

      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :all,
        deliver_policy: :all
      }

      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 5, test_pid: self()})

      assert_receive {:connected, consumer_info}
      assert consumer_info.num_pending == 3
    end

    test "rejects ephemeral consumer with ack_policy :explicit and batch_size > 1" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :explicit,
        deliver_policy: :all
      }

      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: message}, _}} =
               BatchPullConsumer.start_link(consumer: consumer, batch_size: 3, test_pid: self())

      assert message =~ "batch_size > 1 requires ack_policy: :all"
    end

    test "rejects ephemeral consumer with default ack_policy and batch_size > 1" do
      # Consumer defaults to ack_policy: :explicit, so forgetting to set it should fail
      consumer = %Consumer{
        stream_name: @stream_name,
        deliver_policy: :all
      }

      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: message}, _}} =
               BatchPullConsumer.start_link(consumer: consumer, batch_size: 3, test_pid: self())

      assert message =~ "batch_size > 1 requires ack_policy: :all"
    end

    test "durable consumer with ack_policy :explicit fails to connect in batch mode" do
      # Create a durable consumer with explicit ack policy
      consumer = %Consumer{
        stream_name: @stream_name,
        durable_name: "BATCH_EXPLICIT_CONSUMER",
        ack_policy: :explicit
      }

      {:ok, _} = Consumer.create(:gnat, consumer)

      pid =
        start_supervised!(
          {BatchPullConsumer,
           [
             stream_name: @stream_name,
             consumer_name: "BATCH_EXPLICIT_CONSUMER",
             batch_size: 3,
             test_pid: self(),
             connection_retry_timeout: 50,
             connection_retries: 1
           ]},
          restart: :temporary
        )

      ref = Process.monitor(pid)

      # Should fail to connect and eventually stop after retries
      assert_receive {:DOWN, ^ref, :process, ^pid, :timeout}, 5_000
    end

    test "allows batch_size 1 with any ack_policy (no validation needed)" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :explicit,
        deliver_policy: :all
      }

      # batch_size: 1 should not trigger the ack_policy validation
      pid =
        start_supervised!(
          {BatchPullConsumer, consumer: consumer, batch_size: 1, test_pid: self()}
        )

      assert_receive {:connected, _}
      assert Process.alive?(pid)
    end
  end
end
