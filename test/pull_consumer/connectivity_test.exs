defmodule Gnat.Jetstream.PullConsumer.ConnectivityTest do
  use Gnat.Jetstream.ConnCase

  alias Gnat.Jetstream.API.{Consumer, Stream}

  @request_expires 1_000_000_000
  @idle_heartbeat 500_000_000

  defmodule ExamplePullConsumer do
    use Gnat.Jetstream.PullConsumer

    def start_link(opts) do
      Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      {:ok, nil, Keyword.merge([connection_name: :gnat], opts)}
    end

    @impl true
    def handle_message(%{topic: "ackable"}, state) do
      {:ack, state}
    end

    def handle_message(%{topic: "non-ackable", reply_to: reply_to}, state) do
      [_, _, _, _, delivered_count, _, _, _, _] = String.split(reply_to, ".")

      # NACK on first delivery
      if delivered_count == "1" do
        {:nack, state}
      else
        {:ack, state}
      end
    end

    def handle_message(%{topic: "terminatable"}, state) do
      {:term, state}
    end

    def handle_message(%{topic: "skippable"}, state) do
      {:noreply, state}
    end
  end

  defmodule NotifyingPullConsumer do
    use Gnat.Jetstream.PullConsumer

    def start_link(opts) do
      Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      {test_pid, opts} = Keyword.pop!(opts, :test_pid)
      {:ok, test_pid, opts}
    end

    @impl true
    def handle_connected(_info, test_pid) do
      send(test_pid, {:connected, self()})
      {:ok, test_pid}
    end

    @impl true
    def handle_message(message, test_pid) do
      send(test_pid, {:handled, self(), message.body})
      {:ack, test_pid}
    end
  end

  describe "Jetstream.PullConsumer" do
    @describetag with_gnat: :gnat

    setup do
      stream_name = "TEST_STREAM"
      stream_subjects = ["ackable", "non-ackable", "terminatable", "skippable"]
      consumer_name = "TEST_CONSUMER"

      stream = %Stream{name: stream_name, subjects: stream_subjects}
      {:ok, _response} = Stream.create(:gnat, stream)

      consumer = %Consumer{stream_name: stream_name, durable_name: consumer_name}
      {:ok, _response} = Consumer.create(:gnat, consumer)

      on_exit(fn ->
        cleanup()
      end)

      %{
        stream_name: stream_name,
        consumer_name: consumer_name
      }
    end

    test "ignores messages with :noreply", %{
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      start_supervised!(
        {ExamplePullConsumer, stream_name: stream_name, consumer_name: consumer_name}
      )

      :ok = Gnat.pub(:gnat, "skippable", "hello")

      refute_receive {:msg, _}
    end

    test "consumes JetStream messages", %{stream_name: stream_name, consumer_name: consumer_name} do
      start_supervised!(
        {ExamplePullConsumer,
         stream_name: stream_name,
         consumer_name: consumer_name,
         request_expires: @request_expires,
         idle_heartbeat: @idle_heartbeat}
      )

      Gnat.sub(:gnat, self(), "$JS.ACK.#{stream_name}.#{consumer_name}.>")

      :ok = Gnat.pub(:gnat, "ackable", "hello")

      assert_ack_next(stream_name, consumer_name, 1)

      :ok = Gnat.pub(:gnat, "ackable", "hello")

      assert_ack_next(stream_name, consumer_name, 1)

      :ok = Gnat.pub(:gnat, "non-ackable", "hello")

      assert_receive {:msg, %{body: "-NAK", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1")

      assert_ack_next(stream_name, consumer_name, 2)

      :ok = Gnat.pub(:gnat, "ackable", "hello")

      assert_ack_next(stream_name, consumer_name, 1)

      :ok = Gnat.pub(:gnat, "terminatable", "hello")

      assert_receive {:msg, %{body: "+TERM", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1")
    end

    test "can be manually closed", %{stream_name: stream_name, consumer_name: consumer_name} do
      pid =
        start_supervised!(
          {ExamplePullConsumer, stream_name: stream_name, consumer_name: consumer_name}
        )

      ref = Process.monitor(pid)

      assert :ok = Gnat.Jetstream.PullConsumer.close(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    end

    test "retries on unsuccessful connection", %{
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      pid =
        start_supervised!(
          {ExamplePullConsumer,
           connection_name: :non_existent,
           stream_name: stream_name,
           consumer_name: consumer_name,
           connection_retry_timeout: 50,
           connection_retries: 2},
          restart: :temporary
        )

      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :timeout}, 1_000
    end

    test "connects when the connection appears during backoff", %{
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      pid =
        start_notifying(
          connection_name: :late_gnat,
          stream_name: stream_name,
          consumer_name: consumer_name,
          connection_retry_timeout: 50,
          connection_retries: 100
        )

      assert {:disconnected, %{current_retry: 1}} = :sys.get_state(pid)
      start_named_gnat(:late_gnat)
      assert_receive {:connected, ^pid}

      :ok = Gnat.pub(:gnat, "ackable", "hello")
      assert_receive {:handled, ^pid, "hello"}
    end

    test "resets the retry count after connecting", %{
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      pid =
        start_notifying(
          connection_name: :flaky_gnat,
          stream_name: stream_name,
          consumer_name: consumer_name,
          connection_retry_timeout: 200,
          connection_retries: 1
        )

      ref = Process.monitor(pid)
      assert {:disconnected, %{current_retry: 1}} = :sys.get_state(pid)
      start_named_gnat(:flaky_gnat)
      assert_receive {:connected, ^pid}
      assert {:connected, %{current_retry: 0}} = :sys.get_state(pid)

      :ok = stop_supervised(:flaky_gnat)
      await(fn -> match?({:disconnected, %{current_retry: 1}}, :sys.get_state(pid)) end)
      start_named_gnat(:flaky_gnat)

      assert_receive {:connected, ^pid}
      refute_received {:DOWN, ^ref, :process, ^pid, _}
    end

    test "allows setting custom inbox prefix", %{
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      start_supervised!(
        {ExamplePullConsumer,
         inbox_prefix: "CUSTOM_PREFIX.", stream_name: stream_name, consumer_name: consumer_name}
      )

      Gnat.sub(:gnat, self(), "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.#{consumer_name}")

      :ok = Gnat.pub(:gnat, "ackable", "hello")

      expected_body =
        %{batch: 1, idle_heartbeat: 2_500_000_000, expires: 5_000_000_000} |> Jason.encode!()

      assert_receive {:msg, %{body: ^expected_body, reply_to: "CUSTOM_PREFIX." <> _}}
    end
  end

  defp assert_ack_next(stream_name, consumer_name, delivery_count) do
    assert_receive {:msg, %{body: "+NXT " <> payload, topic: topic}}

    assert Jason.decode!(payload) == %{
             "batch" => 1,
             "expires" => @request_expires,
             "idle_heartbeat" => @idle_heartbeat
           }

    assert String.starts_with?(
             topic,
             "$JS.ACK.#{stream_name}.#{consumer_name}.#{delivery_count}"
           )
  end

  defp start_notifying(opts) do
    start_supervised!(
      {NotifyingPullConsumer, Keyword.merge([test_pid: self(), connection_name: :gnat], opts)},
      restart: :temporary
    )
  end

  defp start_named_gnat(name) do
    start_supervised!(%{id: name, start: {Gnat, :start_link, [%{}, [name: name]]}})
  end

  defp await(fun, attempts \\ 100)
  defp await(fun, 0), do: assert(fun.())

  defp await(fun, attempts) do
    unless fun.() do
      Process.sleep(10)
      await(fun, attempts - 1)
    end
  end

  defp cleanup do
    # Manage connection on our own here, because all supervised processes will be
    # closed by the time `on_exit` runs
    {:ok, pid} = Gnat.start_link()
    :ok = Stream.delete(pid, "TEST_STREAM")
    Gnat.stop(pid)
  end
end
