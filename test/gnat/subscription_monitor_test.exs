defmodule Gnat.SubscriptionMonitorTest do
  use ExUnit.Case, async: true

  setup do
    gnat = start_supervised!({Gnat, %{}})
    %{gnat: gnat}
  end

  test "subscriber death removes its subscription", %{gnat: gnat} do
    subscriber = start_subscriber()
    {:ok, sid} = Gnat.sub(gnat, subscriber, "monitors.death")
    ref = monitor_ref(gnat, sid)

    with_suspended(gnat, fn ->
      Process.exit(subscriber, :kill)
      assert_queued_down(gnat, ref, subscriber)
    end)

    refute Map.has_key?(:sys.get_state(gnat).receivers, sid)
    assert {:ok, 1} = Gnat.active_subscriptions(gnat)
  end

  test "explicit unsubscribe flushes a queued subscriber DOWN", %{gnat: gnat} do
    subscriber = start_subscriber()
    {:ok, sid} = Gnat.sub(gnat, subscriber, "monitors.queued")
    ref = monitor_ref(gnat, sid)

    request =
      with_suspended(gnat, fn ->
        request = :gen_server.send_request(gnat, {:unsub, sid, []})
        Process.exit(subscriber, :kill)
        assert_queued_down(gnat, ref, subscriber)
        request
      end)

    assert {:reply, :ok} = :gen_server.wait_response(request, 1_000)
    assert {:ok, 1} = Gnat.active_subscriptions(gnat)
    assert {:monitors, []} = Process.info(gnat, :monitors)
  end

  test "stale DOWN after explicit unsubscribe does not crash the connection", %{gnat: gnat} do
    {:ok, sid} = Gnat.sub(gnat, self(), "monitors.stale")
    ref = monitor_ref(gnat, sid)
    :ok = Gnat.unsub(gnat, sid)

    send(gnat, {:DOWN, ref, :process, self(), :normal})

    assert {:ok, 1} = Gnat.active_subscriptions(gnat)
  end

  test "stale DOWN does not remove a replacement subscription for the same process", %{gnat: gnat} do
    topic = "monitors.replacement"
    {:ok, old_sid} = Gnat.sub(gnat, self(), topic)
    old_ref = monitor_ref(gnat, old_sid)
    :ok = Gnat.unsub(gnat, old_sid)
    {:ok, sid} = Gnat.sub(gnat, self(), topic)
    ref = monitor_ref(gnat, sid)
    assert ref != old_ref

    send(gnat, {:DOWN, old_ref, :process, self(), :normal})

    assert monitor_ref(gnat, sid) == ref
    :ok = Gnat.pub(gnat, topic, "still subscribed")
    assert_receive {:msg, %{sid: ^sid, body: "still subscribed"}}
  end

  test "DOWN matches the exact subscription monitor for a shared recipient", %{gnat: gnat} do
    {:ok, first} = Gnat.sub(gnat, self(), "monitors.first")
    {:ok, second} = Gnat.sub(gnat, self(), "monitors.second")
    first_ref = monitor_ref(gnat, first)
    second_ref = monitor_ref(gnat, second)

    send(gnat, {:DOWN, second_ref, :process, self(), :normal})

    receivers = :sys.get_state(gnat).receivers
    refute Map.has_key?(receivers, second)
    assert receivers[first].monitor_ref == first_ref
    assert {:monitors, [{:process, recipient}]} = Process.info(gnat, :monitors)
    assert recipient == self()
    :ok = Gnat.pub(gnat, "monitors.first", "still subscribed")
    assert_receive {:msg, %{sid: ^first, body: "still subscribed"}}
  end

  test "subscriber death cleans up multiple subscriptions after one is explicitly removed", %{
    gnat: gnat
  } do
    subscriber = start_subscriber()
    sids = for n <- 1..3, do: elem(Gnat.sub(gnat, subscriber, "monitors.multiple.#{n}"), 1)
    refs = Enum.map(sids, &monitor_ref(gnat, &1))

    request =
      with_suspended(gnat, fn ->
        request = :gen_server.send_request(gnat, {:unsub, hd(sids), []})
        Process.exit(subscriber, :kill)
        Enum.each(refs, &assert_queued_down(gnat, &1, subscriber))
        request
      end)

    assert {:reply, :ok} = :gen_server.wait_response(request, 1_000)
    assert {:ok, 1} = Gnat.active_subscriptions(gnat)
    assert {:monitors, []} = Process.info(gnat, :monitors)
  end

  test "limited unsubscribe retains the monitor until the subscriber dies", %{gnat: gnat} do
    subscriber = start_subscriber()
    {:ok, sid} = Gnat.sub(gnat, subscriber, "monitors.limited_death")
    ref = monitor_ref(gnat, sid)
    :ok = Gnat.unsub(gnat, sid, max_messages: 2)
    assert {:monitors, [{:process, ^subscriber}]} = Process.info(gnat, :monitors)

    with_suspended(gnat, fn ->
      Process.exit(subscriber, :kill)
      assert_queued_down(gnat, ref, subscriber)
    end)

    refute Map.has_key?(:sys.get_state(gnat).receivers, sid)
    assert {:ok, 1} = Gnat.active_subscriptions(gnat)
  end

  for {name, opts} <- [{"MSG", []}, {"HMSG", [headers: [{"x-test", "monitor"}]]}] do
    test "limited unsubscribe retires its monitor after the final #{name}", %{gnat: gnat} do
      topic = "monitors.limit.#{unquote(name)}"
      {:ok, sid} = Gnat.sub(gnat, self(), topic)
      ref = monitor_ref(gnat, sid)
      :ok = Gnat.unsub(gnat, sid, max_messages: 2)
      :ok = Gnat.pub(gnat, topic, "first", unquote(opts))
      assert_receive {:msg, %{sid: ^sid, body: "first"}}
      assert monitor_ref(gnat, sid) == ref
      assert {:monitors, [{:process, recipient}]} = Process.info(gnat, :monitors)
      assert recipient == self()

      :ok = Gnat.pub(gnat, topic, "last", unquote(opts))
      assert_receive {:msg, %{sid: ^sid, body: "last"}}
      refute Map.has_key?(:sys.get_state(gnat).receivers, sid)
      assert {:monitors, []} = Process.info(gnat, :monitors)
      send(gnat, {:DOWN, ref, :process, self(), :normal})
      assert {:ok, 1} = Gnat.active_subscriptions(gnat)
    end
  end

  defp monitor_ref(gnat, sid) do
    :sys.get_state(gnat).receivers |> Map.fetch!(sid) |> Map.fetch!(:monitor_ref)
  end

  defp start_subscriber do
    pid = spawn(fn -> receive do: (:stop -> :ok) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp with_suspended(gnat, fun) do
    :ok = :sys.suspend(gnat)

    try do
      fun.()
    after
      :ok = :sys.resume(gnat)
    end
  end

  defp assert_queued_down(gnat, ref, subscriber) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    await_queued_down(gnat, ref, subscriber, deadline)
  end

  defp await_queued_down(gnat, ref, subscriber, deadline) do
    {:messages, messages} = Process.info(gnat, :messages)

    unless Enum.any?(messages, &match?({:DOWN, ^ref, :process, ^subscriber, :killed}, &1)) do
      assert System.monotonic_time(:millisecond) < deadline,
             "subscriber DOWN was not queued: #{inspect(messages)}"

      receive do
      after
        1 -> await_queued_down(gnat, ref, subscriber, deadline)
      end
    end
  end
end
