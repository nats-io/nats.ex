defmodule Gnat.Jetstream.PullConsumer.EphemeralTest do
  use Gnat.Jetstream.ConnCase

  alias Gnat.Jetstream.API.{Consumer, Stream}

  defmodule ExamplePullConsumer do
    use Gnat.Jetstream.PullConsumer

    def start_link(opts) do
      Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      consumer = Keyword.fetch!(opts, :consumer)

      connection_opts = [
        connection_name: :gnat,
        consumer: consumer
      ]

      state = %{test_pid: Keyword.fetch!(opts, :test_pid)}
      {:ok, state, connection_opts}
    end

    @impl true
    def handle_message(message, state) do
      send(state.test_pid, {:pulled, message})
      {:ack, state}
    end
  end

  describe "Jetstream.PullConsumer" do
    @describetag with_gnat: :gnat

    setup do
      stream_name = "TEST_STREAM_2"
      stream_subjects = ["stream_2.*"]

      stream = %Stream{name: stream_name, subjects: stream_subjects}
      {:ok, _response} = Stream.create(:gnat, stream)

      on_exit(fn ->
        cleanup()
      end)

      %{
        stream_name: stream_name
      }
    end

    test "ephemeral consumer receives messages", %{
      stream_name: stream_name
    } do
      consumer = %Consumer{stream_name: stream_name}

      {:ok, _resp} = Gnat.request(:gnat, "stream_2.ohai", "whatsup")

      start_supervised!({ExamplePullConsumer, consumer: consumer, test_pid: self()})

      assert_receive {:pulled, message}
      assert %{topic: "stream_2.ohai", body: "whatsup"} = message

      {:ok, _resp} = Gnat.request(:gnat, "stream_2.ohai", "second")

      assert_receive {:pulled, message}
      assert %{topic: "stream_2.ohai", body: "second"} = message
    end
  end

  defp cleanup do
    # Manage connection on our own here, because all supervised processes will be
    # closed by the time `on_exit` runs
    {:ok, pid} = Gnat.start_link()
    :ok = Stream.delete(pid, "TEST_STREAM_2")
    Gnat.stop(pid)
  end
end
