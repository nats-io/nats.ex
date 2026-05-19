defmodule Gnat.Jetstream.PullConsumer.HeartbeatTest do
  use Gnat.Jetstream.ConnCase

  alias Gnat.Jetstream.API.{Consumer, Stream}
  alias Gnat.Jetstream.PullConsumer.ConnectionOptions

  defmodule IdleConsumer do
    use Gnat.Jetstream.PullConsumer

    def start_link(opts), do: Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)

    @impl true
    def init(opts), do: {:ok, nil, Keyword.merge([connection_name: :gnat], opts)}

    @impl true
    def handle_message(_msg, state), do: {:ack, state}
  end

  describe "ConnectionOptions.validate!/1 — heartbeat options" do
    test "defaults idle_heartbeat to half of request_expires" do
      opts =
        ConnectionOptions.validate!(
          connection_name: :gnat,
          stream_name: "S",
          consumer_name: "C"
        )

      assert opts.idle_heartbeat == div(opts.request_expires, 2)
    end

    test "explicit idle_heartbeat is preserved" do
      opts =
        ConnectionOptions.validate!(
          connection_name: :gnat,
          stream_name: "S",
          consumer_name: "C",
          request_expires: 4_000_000_000,
          idle_heartbeat: 1_000_000_000
        )

      assert opts.idle_heartbeat == 1_000_000_000
    end

    test "rejects idle_heartbeat > request_expires / 2" do
      assert_raise ArgumentError, ~r/idle_heartbeat.*must be at most half of/, fn ->
        ConnectionOptions.validate!(
          connection_name: :gnat,
          stream_name: "S",
          consumer_name: "C",
          request_expires: 1_000_000_000,
          idle_heartbeat: 800_000_000
        )
      end
    end

    test "accepts idle_heartbeat exactly equal to request_expires / 2" do
      opts =
        ConnectionOptions.validate!(
          connection_name: :gnat,
          stream_name: "S",
          consumer_name: "C",
          request_expires: 2_000_000_000,
          idle_heartbeat: 1_000_000_000
        )

      assert opts.idle_heartbeat == 1_000_000_000
    end

    test "defaults heartbeat_check_interval to 1 second" do
      opts =
        ConnectionOptions.validate!(
          connection_name: :gnat,
          stream_name: "S",
          consumer_name: "C"
        )

      assert opts.heartbeat_check_interval == 1_000
    end
  end

  describe "watchdog behavior — single-message mode (batch_size: 1)" do
    @describetag with_gnat: :gnat

    setup do
      stream_name = "HB_TEST_STREAM"
      consumer_name = "HB_TEST_CONSUMER"

      stream = %Stream{name: stream_name, subjects: ["hb.test"]}
      {:ok, _} = Stream.create(:gnat, stream)

      consumer = %Consumer{stream_name: stream_name, durable_name: consumer_name}
      {:ok, _} = Consumer.create(:gnat, consumer)

      on_exit(fn ->
        cleanup(stream_name)
      end)

      %{stream_name: stream_name, consumer_name: consumer_name}
    end

    @tag :integration
    test "does not fire watchdog on idle stream", %{
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      test_pid = self()

      :ok =
        :telemetry.attach(
          "hb-test-#{System.unique_integer()}",
          [:gnat, :jetstream, :pull_consumer, :heartbeat_expired],
          fn _e, m, meta, _ -> send(test_pid, {:watchdog, m, meta}) end,
          nil
        )

      # Tight timings so we don't have to wait long, but still respect
      # idle_heartbeat <= request_expires / 2.
      start_supervised!(
        {IdleConsumer,
         stream_name: stream_name,
         consumer_name: consumer_name,
         request_expires: 1_000_000_000,
         idle_heartbeat: 500_000_000,
         heartbeat_check_interval: 200}
      )

      # Watchdog threshold is 2 * 500ms = 1000ms. If the bug were present,
      # we'd see a fire within ~1.2s. Wait long enough for two threshold
      # windows to confirm the watchdog stays quiet.
      refute_receive {:watchdog, _, _}, 3_000
    end
  end

  defp cleanup(stream_name) do
    {:ok, pid} = Gnat.start_link()
    _ = Stream.delete(pid, stream_name)
    Gnat.stop(pid)
  end
end
