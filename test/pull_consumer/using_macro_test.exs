defmodule Gnat.Jetstream.PullConsumer.UsingMacroTest do
  use Gnat.Jetstream.ConnCase

  defmodule ExamplePullConsumer do
    use Gnat.Jetstream.PullConsumer, restart: :temporary, shutdown: 12345

    def start_link(_) do
      Gnat.Jetstream.PullConsumer.start_link(__MODULE__, [])
    end

    @impl true
    def init([]) do
      {:ok, nil, []}
    end

    @impl true
    def handle_message(%{}, state) do
      {:ack, state}
    end
  end

  describe "use Jetstream.PullConsumer" do
    test "allows specifing child specification options via argument" do
      assert ExamplePullConsumer.child_spec(:arg) == %{
               id: ExamplePullConsumer,
               start: {ExamplePullConsumer, :start_link, [:arg]},
               restart: :temporary,
               shutdown: 12345
             }
    end
  end
end
