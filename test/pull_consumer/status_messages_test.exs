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
  end
end
