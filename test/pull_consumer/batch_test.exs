defmodule Gnat.Jetstream.PullConsumer.BatchTest do
  use Gnat.Jetstream.ConnCase

  alias Gnat.Jetstream.API.{Consumer, Stream}

  @stream_name "BATCH_TEST_STREAM"
  @subject "batch_test.*"

  defmodule ControlledConsumer do
    use Gnat.Jetstream.PullConsumer

    def start_link(opts), do: Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      {test_pid, opts} = Keyword.pop!(opts, :test_pid)
      {:ok, {test_pid, 0}, Keyword.put(opts, :connection_name, :gnat)}
    end

    @impl true
    def handle_connected(_info, {test_pid, _count} = state) do
      send(test_pid, {:connected, self()})
      {:ok, state}
    end

    @impl true
    def handle_status(message, {test_pid, _count} = state) do
      send(test_pid, {:status, self(), message.status})
      {:ok, state}
    end

    @impl true
    def handle_message(message, {test_pid, count}) do
      send(test_pid, {:handling, self(), count + 1, message})

      receive do
        :raise ->
          raise "handler failed"

        :exit ->
          exit(:handler_exit)

        action when action in [:ack, :nack, :term, :noreply, :invalid] ->
          {action, {test_pid, count + 1}}
      end
    end
  end

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

  defp create_durable(name, opts \\ []) do
    {:ok, _} =
      Consumer.create(
        :gnat,
        struct!(
          %Consumer{
            stream_name: @stream_name,
            durable_name: name,
            ack_policy: :explicit
          },
          opts
        )
      )

    name
  end

  defp publish_messages(bodies) do
    for body <- bodies do
      {:ok, _} = Gnat.request(:gnat, "batch_test.regression", body)
    end
  end

  defp start_controlled(consumer, batch_size, id \\ ControlledConsumer, opts \\ []) do
    consumer_opts =
      case consumer do
        %Consumer{} -> [consumer: consumer]
        name -> [stream_name: @stream_name, consumer_name: name]
      end

    start_supervised!(
      {ControlledConsumer,
       Keyword.merge(
         [
           test_pid: self(),
           batch_size: batch_size,
           request_expires: 500_000_000
         ] ++ consumer_opts,
         opts
       )},
      id: id,
      restart: :temporary
    )
  end

  defp reconnect(_pid, :connection_down) do
    Process.exit(Process.whereis(:gnat), :kill)
  end

  defp reconnect(pid, :heartbeat_expired) do
    :sys.replace_state(pid, fn state ->
      put_in(state.mod_state.last_response_at, System.monotonic_time(:millisecond) - 2_000)
    end)

    send(pid, :heartbeat_check)
  end

  defp await_buffer(pid, count, retries \\ 100) do
    %{mod_state: %{buffer: buffer}} = :sys.get_state(pid)

    if length(buffer) == count or retries == 0 do
      assert length(buffer) == count
    else
      Process.sleep(10)
      await_buffer(pid, count, retries - 1)
    end
  end

  defp await_pending(consumer, expected, retries \\ 100) do
    {:ok, info} = Consumer.info(:gnat, @stream_name, consumer)

    if info.num_ack_pending == expected or retries == 0 do
      assert info.num_ack_pending == expected
      info
    else
      Process.sleep(10)
      await_pending(consumer, expected, retries - 1)
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

    test "workers sharing a durable consumer only acknowledge their own completed messages" do
      consumer = create_durable("shared")
      publish_messages(["1", "2", "3", "4"])

      slow = start_controlled(consumer, 2, :slow)
      assert_receive {:handling, ^slow, 1, %{body: "1"}}

      fast = start_controlled(consumer, 2, :fast)
      assert_receive {:handling, ^fast, 1, %{body: "3"}}
      send(fast, :ack)
      assert_receive {:handling, ^fast, 2, %{body: "4"}}
      send(fast, :ack)

      info = await_pending(consumer, 2)
      assert info.ack_floor.stream_seq == 0
      refute_receive {:handling, ^slow, 2, _}

      send(slow, :ack)
      assert_receive {:handling, ^slow, 2, %{body: "2"}}
      info = await_pending(consumer, 1)
      assert info.ack_floor.stream_seq == 1

      send(slow, :ack)
      info = await_pending(consumer, 0)
      assert info.ack_floor.stream_seq == 4
    end

    for batch_size <- [4, 6] do
      @tag batch_size: batch_size
      test "preserves handler outcomes with batch_size #{batch_size}", %{batch_size: batch_size} do
        consumer = create_durable("outcomes")
        {:ok, _} = Gnat.sub(:gnat, self(), "$JS.ACK.#{@stream_name}.#{consumer}.>")
        publish_messages(["nack", "term", "noreply", "ack"])
        pid = start_controlled(consumer, batch_size)

        assert_receive {:handling, ^pid, 1, %{body: "nack", reply_to: nack_topic}}
        send(pid, :nack)
        assert_receive {:handling, ^pid, 2, %{body: "term", reply_to: term_topic}}
        send(pid, :term)
        assert_receive {:handling, ^pid, 3, %{body: "noreply", reply_to: noreply_topic}}
        send(pid, :noreply)
        assert_receive {:handling, ^pid, 4, %{body: "ack", reply_to: ack_topic}}
        send(pid, :ack)

        assert_receive {:msg, %{topic: ^nack_topic, body: "-NAK"}}
        assert_receive {:msg, %{topic: ^term_topic, body: "+TERM"}}
        assert_receive {:msg, %{topic: ^ack_topic, body: ""}}
        assert_receive {:handling, ^pid, 5, %{body: "nack"}}, 3_000
        await_pending(consumer, 2)
        refute_receive {:msg, %{topic: ^noreply_topic}}

        send(pid, :ack)
        await_pending(consumer, 1)
      end
    end

    for {action, exception} <- [raise: RuntimeError, invalid: CaseClauseError] do
      @tag handler_action: action, handler_exception: exception
      test "handler #{action} leaves failed and unprocessed messages pending", %{
        handler_action: action,
        handler_exception: exception
      } do
        consumer = create_durable("failure")
        publish_messages(["1", "2", "3"])
        pid = start_controlled(consumer, 3)
        ref = Process.monitor(pid)

        assert_receive {:handling, ^pid, 1, %{body: "1"}}
        send(pid, :ack)
        assert_receive {:handling, ^pid, 2, %{body: "2"}}
        send(pid, action)
        assert_receive {:DOWN, ^ref, :process, ^pid, {reason, stacktrace}}
        assert %{__struct__: ^exception} = Exception.normalize(:error, reason, stacktrace)

        info = await_pending(consumer, 2)
        assert info.ack_floor.stream_seq == 1
        refute_receive {:handling, ^pid, 3, _}
      end
    end

    for reason <- [:connection_down, :heartbeat_expired],
        mode <- [:durable, :ephemeral, :limited] do
      @tag reconnect_reason: reason, consumer_mode: mode
      test "processes the #{mode} partial buffer before #{reason}", %{
        reconnect_reason: reason,
        consumer_mode: mode
      } do
        consumer =
          case mode do
            :durable -> create_durable("reconnect")
            :limited -> create_durable("reconnect", max_deliver: 1)
            :ephemeral -> %Consumer{stream_name: @stream_name, deliver_policy: :new}
          end

        pid = start_controlled(consumer, 3, ControlledConsumer, request_expires: 1_000_000_000)
        assert_receive {:connected, ^pid}
        assert_receive {:status, ^pid, "404"}

        publish_messages(["1", "2"])
        await_buffer(pid, 2)
        reconnect(pid, reason)

        assert_receive {:handling, ^pid, 1, %{body: "1"}}, 3_000
        send(pid, :ack)
        assert_receive {:handling, ^pid, 2, %{body: "2"}}
        send(pid, :term)
        assert_receive {:connected, ^pid}, 3_000
        await_buffer(pid, 0)

        publish_messages(["3"])
        assert_receive {:handling, ^pid, 3, %{body: "3"}}, 3_000
        send(pid, :ack)
      end
    end

    test "preserves negative acknowledgements and noreply while flushing a partial buffer" do
      consumer = create_durable("reset_outcomes")
      {:ok, _} = Gnat.sub(:gnat, self(), "$JS.ACK.#{@stream_name}.#{consumer}.>")
      pid = start_controlled(consumer, 3, ControlledConsumer, request_expires: 1_000_000_000)
      assert_receive {:connected, ^pid}
      assert_receive {:status, ^pid, "404"}
      publish_messages(["nack", "noreply"])
      await_buffer(pid, 2)
      reconnect(pid, :heartbeat_expired)

      assert_receive {:handling, ^pid, 1, %{body: "nack", reply_to: nack_topic}}
      send(pid, :nack)
      assert_receive {:handling, ^pid, 2, %{body: "noreply", reply_to: noreply_topic}}
      send(pid, :noreply)
      assert_receive {:connected, ^pid}
      assert_receive {:msg, %{topic: ^nack_topic, body: "-NAK"}}
      assert_receive {:handling, ^pid, 3, %{body: "nack"}}
      send(pid, :ack)
      await_pending(consumer, 1)
      refute_receive {:msg, %{topic: ^noreply_topic}}
    end

    test "does not swallow handler exits while flushing a partial buffer" do
      consumer = create_durable("reset_failure")
      pid = start_controlled(consumer, 3, ControlledConsumer, request_expires: 1_000_000_000)
      ref = Process.monitor(pid)
      assert_receive {:connected, ^pid}
      assert_receive {:status, ^pid, "404"}
      publish_messages(["1", "2"])
      await_buffer(pid, 2)
      reconnect(pid, :heartbeat_expired)

      assert_receive {:handling, ^pid, 1, %{body: "1"}}
      send(pid, :exit)
      assert_receive {:DOWN, ^ref, :process, ^pid, :handler_exit}
      await_pending(consumer, 2)
      refute_receive {:handling, ^pid, 2, _}
      refute_receive {:connected, ^pid}
    end

    test "processes a full batch of messages" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :explicit,
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
        ack_policy: :explicit,
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
        ack_policy: :explicit,
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
        ack_policy: :explicit,
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
        ack_policy: :explicit,
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

    test "consumer does not get stuck after processing batch" do
      # This test verifies the critical flow: after a batch is processed and acked,
      # the consumer must issue another fetch request. If it doesn't, messages
      # published after the batch will never arrive.
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :explicit,
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
      assert_receive {:msg, %{body: "+NXT " <> payload}}, 5_000

      assert Jason.decode!(payload) == %{
               "batch" => 1,
               "expires" => 500_000_000,
               "idle_heartbeat" => 250_000_000
             }
    end

    test "batch mode with batch_size 2 and odd number of messages" do
      consumer = %Consumer{
        stream_name: @stream_name,
        ack_policy: :explicit,
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
        ack_policy: :explicit,
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
        ack_policy: :explicit,
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
        ack_policy: :explicit,
        deliver_policy: :all
      }

      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 5, test_pid: self()})

      assert_receive {:connected, consumer_info}
      assert consumer_info.num_pending == 3
    end

    for policy <- [:all, :none] do
      @tag policy: policy
      test "rejects ephemeral consumer with ack_policy #{policy} in batch mode", %{policy: policy} do
        consumer = %Consumer{stream_name: @stream_name, ack_policy: policy}

        assert_raise ArgumentError, ~r/batch_size > 1 requires ack_policy: :explicit/, fn ->
          Gnat.Jetstream.PullConsumer.ConnectionOptions.validate!(
            connection_name: :gnat,
            consumer: consumer,
            batch_size: 3
          )
        end
      end

      @tag policy: policy
      test "rejects durable consumer with ack_policy #{policy} in batch mode", %{policy: policy} do
        consumer_name = "BATCH_INVALID_POLICY"

        {:ok, _} =
          Consumer.create(:gnat, %Consumer{
            stream_name: @stream_name,
            durable_name: consumer_name,
            ack_policy: policy
          })

        {:ok, _} =
          Gnat.sub(:gnat, self(), "$JS.API.CONSUMER.MSG.NEXT.#{@stream_name}.#{consumer_name}")

        pid =
          start_supervised!(
            {BatchPullConsumer,
             stream_name: @stream_name,
             consumer_name: consumer_name,
             batch_size: 3,
             test_pid: self(),
             connection_retry_timeout: 50,
             connection_retries: 1},
            restart: :temporary
          )

        ref = Process.monitor(pid)

        assert_receive {:DOWN, ^ref, :process, ^pid, :timeout}, 5_000
        refute_receive {:connected, _}
        refute_receive {:msg, %{topic: "$JS.API.CONSUMER.MSG.NEXT." <> _}}
      end
    end

    test "accepts the default explicit ack policy in batch mode" do
      consumer = %Consumer{stream_name: @stream_name}
      publish_messages(["default"])
      start_supervised!({BatchPullConsumer, consumer: consumer, batch_size: 3, test_pid: self()})
      assert_receive {:connected, %{name: name}}
      assert_receive {:handled, 1, "default"}
      await_pending(name, 0)
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
