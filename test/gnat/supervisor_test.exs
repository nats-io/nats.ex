defmodule Gnat.SupervisorTest do
  use ExUnit.Case, async: true

  defmodule MyApp.Gnat do
    use Gnat.Supervisor, otp_app: :gnat
  end

  test "it can be supervised" do
    assert MyApp.Gnat.child_spec([]) == %{
      id: MyApp.Gnat,
      start: {MyApp.Gnat, :start_link, [[]]},
      type: :supervisor
    }
  end

  test "when started - it provides an API that does not require a Gnat connection arg" do
    {:ok, _pid} = MyApp.Gnat.start_link([])
    :timer.sleep(100)
    {:ok, _sub} = MyApp.Gnat.sub(self(), "my_app.topic")
    :ok = MyApp.Gnat.pub("my_app.topic", "ohai")
    assert_receive {:msg, %{topic: "my_app.topic", body: "ohai"}}
    assert Supervisor.stop(MyApp.Gnat) == :ok
  end
end
