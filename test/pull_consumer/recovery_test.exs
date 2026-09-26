defmodule Gnat.Jetstream.PullConsumer.RecoveryTest do
  use Gnat.Jetstream.ConnCase

  alias Gnat.Jetstream.API.{Consumer, Stream}

  @moduletag with_gnat: :gnat

  defmodule ControlledConsumer do
    use Gnat.Jetstream.PullConsumer

    def start_link(opts), do: Gnat.Jetstream.PullConsumer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      {test_pid, opts} = Keyword.pop!(opts, :test_pid)
      {kill_on_connect, opts} = Keyword.pop(opts, :kill_on_connect, false)
      {:ok, {test_pid, 0, kill_on_connect}, Keyword.put(opts, :connection_name, :gnat)}
    end

    @impl true
    def handle_connected(info, {test_pid, count, kill_on_connect}) do
      send(test_pid, {:connected, self(), info.name})

      if kill_on_connect do
        conn = Process.whereis(:gnat)
        ref = Process.monitor(conn)
        Process.exit(conn, :kill)

        receive do
          {:DOWN, ^ref, :process, ^conn, _} -> :ok
        end
      end

      {:ok, {test_pid, count, false}}
    end

    @impl true
    def handle_status(%{description: "block_status"}, {test_pid, _, _} = state) do
      send(test_pid, {:status_callback, self()})

      receive do
        :release_status -> {:ok, state}
      end
    end

    def handle_status(_message, state), do: {:ok, state}

    @impl true
    def handle_message(message, {test_pid, count, kill_on_connect}) do
      send(test_pid, {:handling, self(), count + 1, message})

      receive do
        {:return, action} -> {action, {test_pid, count + 1, kill_on_connect}}
      end
    end
  end

  setup do
    stream = "RECOVERY_#{System.unique_integer([:positive])}"
    {:ok, _} = Stream.create(:gnat, %Stream{name: stream, subjects: [stream]})

    on_exit(fn ->
      {:ok, conn} = Gnat.start_link()
      Stream.delete(conn, stream)
      Gnat.stop(conn)
    end)

    %{stream: stream}
  end

  for mode <- [:ephemeral, :durable, :managed_durable], reason <- [:watchdog, :connection_down] do
    @tag mode: mode, reason: reason
    test "#{mode} retains consumer identity after #{reason}", context do
      %{stream: stream, mode: mode, reason: reason} = context
      pid = start_consumer(stream, mode)
      assert_receive {:connected, ^pid, name}
      publish(stream, "first")
      assert_receive {:handling, ^pid, 1, %{body: "first"}}
      send(pid, {:return, :ack})

      await(fn ->
        {:ok, info} = Consumer.info(:gnat, stream, name)
        info.num_ack_pending == 0
      end)

      case reason do
        :watchdog -> expire(pid)
        :connection_down -> Process.exit(Process.whereis(:gnat), :kill)
      end

      assert_receive {:connected, ^pid, new_name}, 3_000
      assert new_name == name
      publish(stream, "second")
      assert_receive {:handling, ^pid, 2, %{body: "second"}}
      send(pid, {:return, :ack})
    end
  end

  for batch_size <- [1, 3], action <- [:ack, :nack, :term, :noreply] do
    @tag batch_size: batch_size, action: action
    test "recovery processes queued messages with #{action} at batch size #{batch_size}", %{
      stream: stream,
      batch_size: batch_size,
      action: action
    } do
      pid = start_consumer(stream, :ephemeral, batch_size: batch_size, deliver_policy: :new)
      assert_receive {:connected, ^pid, name}

      await(fn ->
        {:ok, info} = Consumer.info(:gnat, stream, name)
        info.num_waiting == 1
      end)

      {:ok, _} = Gnat.sub(:gnat, self(), "$JS.ACK.#{stream}.>")
      :sys.suspend(pid)
      expire_state(pid)
      send(pid, :heartbeat_check)
      publish(stream, "queued")

      await(fn ->
        {:messages, messages} = Process.info(pid, :messages)
        Enum.any?(messages, &match?({:msg, %{body: "queued"}}, &1))
      end)

      :sys.resume(pid)

      assert_receive {:handling, ^pid, 1, %{body: "queued", reply_to: reply}}
      send(pid, {:return, action})
      assert_receive {:connected, ^pid, ^name}

      case action do
        :ack -> assert_receive {:msg, %{topic: ^reply, body: ""}}
        :nack -> assert_receive {:msg, %{topic: ^reply, body: "-NAK"}}
        :term -> assert_receive {:msg, %{topic: ^reply, body: "+TERM"}}
        :noreply -> refute_receive {:msg, %{topic: ^reply}}
      end

      publish(stream, "after")
      assert_receive {:handling, ^pid, 2, %{body: "after"}}, 3_000
      send(pid, {:return, :ack})
    end
  end

  for batch_size <- [1, 2] do
    @tag batch_size: batch_size
    test "slow callbacks don't expire the watchdog at batch size #{batch_size}", %{
      stream: stream,
      batch_size: batch_size
    } do
      pid =
        start_consumer(stream, :ephemeral, batch_size: batch_size, idle_heartbeat: 100_000_000)

      assert_receive {:connected, ^pid, name}
      for i <- 1..batch_size, do: publish(stream, Integer.to_string(i))
      assert_receive {:handling, ^pid, 1, _}
      send(pid, :heartbeat_check)
      Process.sleep(250)
      send(pid, {:return, :ack})

      if batch_size == 2 do
        assert_receive {:handling, ^pid, 2, _}
        send(pid, {:return, :ack})
      end

      # The state call runs after the queued watchdog check.
      %{mod_state: state} = :sys.get_state(pid)
      assert state.consumer_name == name
      refute_receive {:connected, ^pid, _}, 100
    end
  end

  for batch_size <- [1, 3],
      {status, description} <- [{"404", "No Messages"}, {"409", "Leadership Change"}] do
    @tag batch_size: batch_size, status: status, description: description
    test "#{status} #{description} keeps the subscription at batch size #{batch_size}", %{
      stream: stream,
      batch_size: batch_size,
      status: status,
      description: description
    } do
      pid = start_consumer(stream, :ephemeral, batch_size: batch_size)
      assert_receive {:connected, ^pid, name}

      await(fn ->
        {:ok, info} = Consumer.info(:gnat, stream, name)
        info.num_waiting == 1
      end)

      %{mod_state: before} = :sys.get_state(pid)
      {:ok, _} = Gnat.sub(:gnat, self(), "$JS.API.CONSUMER.MSG.NEXT.#{stream}.#{name}")

      send(
        pid,
        {:msg,
         %{
           gnat: before.connection_pid,
           sid: before.subscription_id,
           status: status,
           description: description,
           body: ""
         }}
      )

      assert_receive {:msg, %{reply_to: inbox}}
      assert inbox == before.listening_topic
      %{mod_state: after_status} = :sys.get_state(pid)
      assert after_status.subscription_id == before.subscription_id
      assert after_status.consumer_name == name
      refute_received {:connected, ^pid, _}

      for i <- 1..batch_size, do: publish(stream, Integer.to_string(i))

      for i <- 1..batch_size do
        body = Integer.to_string(i)
        assert_receive {:handling, ^pid, ^i, %{body: ^body}}
        send(pid, {:return, :ack})
      end
    end
  end

  test "recreates an owned consumer only after the server deleted it", %{stream: stream} do
    pid = start_consumer(stream, :ephemeral)
    assert_receive {:connected, ^pid, name}

    await(fn ->
      {:ok, info} = Consumer.info(:gnat, stream, name)
      info.num_waiting == 1
    end)

    assert :ok = Consumer.delete(:gnat, stream, name)
    assert_receive {:connected, ^pid, replacement}, 3_000
    assert replacement != name
    publish(stream, "replacement")
    assert_receive {:handling, ^pid, 1, %{body: "replacement"}}
    send(pid, {:return, :ack})
  end

  for batch_size <- [1, 2], policy <- [:explicit, :all], timing <- [:queued, :handling] do
    @tag batch_size: batch_size, policy: policy, timing: timing
    test "connection death while #{timing} preserves deliveries for #{policy} at batch size #{batch_size}",
         %{
           stream: stream,
           batch_size: batch_size,
           policy: policy,
           timing: timing
         } do
      pid = start_consumer(stream, :ephemeral, batch_size: batch_size, ack_policy: policy)
      assert_receive {:connected, ^pid, name}

      await(fn ->
        {:ok, info} = Consumer.info(:gnat, stream, name)
        info.num_waiting == 1
      end)

      :sys.suspend(pid)
      for i <- 1..batch_size, do: publish(stream, Integer.to_string(i))

      await(fn ->
        {:messages, messages} = Process.info(pid, :messages)
        Enum.count(messages, &match?({:msg, %{body: body}} when body != "", &1)) == batch_size
      end)

      if timing == :handling do
        :sys.resume(pid)
        assert_receive {:handling, ^pid, 1, %{body: "1"}}
      end

      conn = Process.whereis(:gnat)
      ref = Process.monitor(conn)
      Process.exit(conn, :kill)
      assert_receive {:DOWN, ^ref, :process, ^conn, _}
      if timing == :queued, do: :sys.resume(pid)

      for i <- 1..batch_size do
        body = Integer.to_string(i)

        unless timing == :handling and i == 1 do
          assert_receive {:handling, ^pid, ^i, %{body: ^body}}
        end

        send(pid, {:return, :ack})
      end

      assert_receive {:connected, ^pid, ^name}, 3_000
      publish(stream, "after")
      next = batch_size + 1
      assert_receive {:handling, ^pid, ^next, %{body: "after"}}, 3_000
      send(pid, {:return, :ack})
    end
  end

  test "a deleted externally managed consumer isn't recreated", %{stream: stream} do
    pid = start_consumer(stream, :durable, connection_retries: 2)
    ref = Process.monitor(pid)
    assert_receive {:connected, ^pid, name}

    await(fn ->
      {:ok, info} = Consumer.info(:gnat, stream, name)
      info.num_waiting == 1
    end)

    assert :ok = Consumer.delete(:gnat, stream, name)
    assert_receive {:DOWN, ^ref, :process, ^pid, :timeout}, 3_000
    assert {:error, %{"err_code" => 10014}} = Consumer.info(:gnat, stream, name)
  end

  for policy <- [:explicit, :all] do
    @tag policy: policy
    test "#{policy} recovery preserves order across the buffer and mailbox", %{
      stream: stream,
      policy: policy
    } do
      pid = start_consumer(stream, :ephemeral, batch_size: 3, ack_policy: policy)
      assert_receive {:connected, ^pid, name}

      await(fn ->
        {:ok, info} = Consumer.info(:gnat, stream, name)
        info.num_waiting == 1
      end)

      publish(stream, "buffered")

      await(fn ->
        %{mod_state: state} = :sys.get_state(pid)
        length(state.buffer) == 1
      end)

      :sys.suspend(pid)
      expire_state(pid)
      send(pid, :heartbeat_check)
      publish(stream, "queued")

      await(fn ->
        {:messages, messages} = Process.info(pid, :messages)
        Enum.any?(messages, &match?({:msg, %{body: "queued"}}, &1))
      end)

      :sys.resume(pid)
      assert_receive {:handling, ^pid, 1, %{body: "buffered"}}
      send(pid, {:return, :ack})
      assert_receive {:handling, ^pid, 2, %{body: "queued"}}
      send(pid, {:return, :ack})
      assert_receive {:connected, ^pid, ^name}

      await(fn ->
        {:ok, info} = Consumer.info(:gnat, stream, name)
        info.num_ack_pending == 0
      end)
    end
  end

  test "failed initial pull preserves consumer identity and callback state", %{stream: stream} do
    pid = start_consumer(stream, :ephemeral, kill_on_connect: true)
    assert_receive {:connected, ^pid, name}
    assert_receive {:connected, ^pid, ^name}, 3_000
    publish(stream, "after")
    assert_receive {:handling, ^pid, 1, %{body: "after"}}
    send(pid, {:return, :ack})
    :sys.get_state(pid)
    conn = Process.whereis(:gnat)
    assert {:monitors, [{:process, ^conn}]} = Process.info(pid, :monitors)
  end

  test "messages from a retired subscription can't change the replacement pull", %{stream: stream} do
    pid = start_consumer(stream, :ephemeral, batch_size: 3)
    assert_receive {:connected, ^pid, name}
    %{mod_state: old} = :sys.get_state(pid)
    expire(pid)
    assert_receive {:connected, ^pid, ^name}

    await(fn ->
      {:ok, info} = Consumer.info(:gnat, stream, name)
      info.num_waiting == 1
    end)

    stale = %{gnat: old.connection_pid, sid: old.subscription_id, body: "ignored", topic: stream}
    send(pid, {:msg, stale})
    send(pid, {:msg, Map.merge(stale, %{status: "409", description: "Consumer Deleted"})})
    %{mod_state: state} = :sys.get_state(pid)
    assert state.buffer == []
    assert state.consumer_name == name
    assert state.connection_pid == old.connection_pid
    assert state.subscription_id != old.subscription_id
    refute_receive {:connected, ^pid, _}, 100
  end

  test "slow status callbacks don't expire the watchdog", %{stream: stream} do
    pid = start_consumer(stream, :ephemeral, idle_heartbeat: 100_000_000)
    assert_receive {:connected, ^pid, name}
    %{mod_state: state} = :sys.get_state(pid)

    send(
      pid,
      {:msg,
       %{
         gnat: state.connection_pid,
         sid: state.subscription_id,
         status: "100",
         description: "block_status",
         body: ""
       }}
    )

    assert_receive {:status_callback, ^pid}
    send(pid, :heartbeat_check)
    Process.sleep(250)
    send(pid, :release_status)
    %{mod_state: state} = :sys.get_state(pid)
    assert state.consumer_name == name
    refute_receive {:connected, ^pid, _}, 100
  end

  @tag :transport_timeout
  test "a late subscription reply doesn't leave an orphan subscription", %{stream: stream} do
    conn = Process.whereis(:gnat)
    handler = pause_subscription(stream, conn)

    try do
      pid = start_consumer(stream, :durable)
      assert_receive :subscription_paused
      Process.sleep(5_100)
      :sys.resume(conn)
      assert_receive {:connected, ^pid, _}, 3_000
      assert {:ok, 2} = Gnat.active_subscriptions(conn)
      assert {:monitors, [{:process, ^conn}]} = Process.info(pid, :monitors)
    after
      :telemetry.detach(handler)
      :sys.resume(conn)
    end
  end

  @tag :transport_timeout
  test "deliveries forwarded after five seconds of unsubscribe delay are preserved", %{
    stream: stream
  } do
    pid = start_consumer(stream, :managed_durable, request_expires: 20_000_000_000)
    assert_receive {:connected, ^pid, name}

    await(fn ->
      {:ok, info} = Consumer.info(:gnat, stream, name)
      info.num_waiting == 1
    end)

    {:ok, publisher} = Gnat.start_link()
    conn = Process.whereis(:gnat)
    :sys.suspend(pid)
    expire_state(pid)
    :sys.suspend(conn)

    try do
      assert {:ok, _} = Gnat.request(publisher, stream, "delayed")

      await(fn ->
        {:messages, messages} = Process.info(conn, :messages)
        packets = for {:tcp, _, data} <- messages, do: data
        :binary.match(IO.iodata_to_binary(packets), "delayed") != :nomatch
      end)

      send(pid, :heartbeat_check)
      :sys.resume(pid)
      await(fn -> pending_calls(conn, :unsub) == 1 end)
      Process.sleep(5_100)
      :sys.resume(conn)
      assert_receive {:handling, ^pid, 1, %{body: "delayed"}}, 3_000
      send(pid, {:return, :ack})
      assert_receive {:connected, ^pid, ^name}, 3_000
      publish(stream, "after")
      assert_receive {:handling, ^pid, 2, %{body: "after"}}
      send(pid, {:return, :ack})
    after
      :sys.resume(conn)
      :sys.resume(pid)
      Gnat.stop(publisher)
    end
  end

  for timing <- [:batch, :recovery] do
    @tag transport_timeout: true, timing: timing
    test "an acknowledgement timeout while #{timing} doesn't delay every remaining callback", %{
      stream: stream,
      timing: timing
    } do
      batch_size = if timing == :batch, do: 3, else: 4
      pid = start_consumer(stream, :managed_durable, batch_size: batch_size)
      assert_receive {:connected, ^pid, name}

      await(fn ->
        {:ok, info} = Consumer.info(:gnat, stream, name)
        info.num_waiting == 1
      end)

      :sys.suspend(pid)
      for i <- 1..3, do: publish(stream, Integer.to_string(i))

      await(fn ->
        {:messages, messages} = Process.info(pid, :messages)
        Enum.count(messages, &match?({:msg, %{body: body}} when body != "", &1)) == 3
      end)

      :sys.resume(pid)

      if timing == :recovery do
        await(fn ->
          %{mod_state: state} = :sys.get_state(pid)
          length(state.buffer) == 3
        end)

        expire(pid)
      end

      assert_receive {:handling, ^pid, 1, %{body: "1"}}
      conn = Process.whereis(:gnat)
      :sys.suspend(conn)

      try do
        send(pid, {:return, :ack})
        assert_receive {:handling, ^pid, 2, %{body: "2"}}, 6_000
        send(pid, {:return, :ack})
        assert_receive {:handling, ^pid, 3, %{body: "3"}}
        send(pid, {:return, :ack})
        assert pending_calls(conn, :pub) == 1
        :sys.resume(conn)
        assert_receive {:connected, ^pid, ^name}, 3_000
        %{mod_state: state} = :sys.get_state(pid)
        assert {_, 3, _} = state.state
      after
        :sys.resume(conn)
      end
    end
  end

  for operation <- [:subscribe, :unsubscribe] do
    @tag subscription_operation: operation
    test "connection death during #{operation} releases the pending operation", %{
      stream: stream,
      subscription_operation: operation
    } do
      {pid, conn, name} = pending_subscription(stream, operation)
      ref = Process.monitor(conn)
      Process.exit(conn, :kill)
      assert_receive {:DOWN, ^ref, :process, ^conn, _}
      assert_receive {:connected, ^pid, ^name}, 3_000
      replacement = Process.whereis(:gnat)
      assert replacement != conn
      assert {:ok, 2} = Gnat.active_subscriptions(replacement)
      assert {:monitors, [{:process, ^replacement}]} = Process.info(pid, :monitors)
    end
  end

  for operation <- [:subscribe, :unsubscribe, :connected] do
    @tag subscription_operation: operation, pending_close: true
    test "close during #{operation} stops without waiting for Gnat and cleans up on resume", %{
      stream: stream,
      subscription_operation: operation
    } do
      {pid, conn, _name} = pending_subscription(stream, operation)
      ref = Process.monitor(pid)
      task = Task.async(fn -> Gnat.Jetstream.PullConsumer.close(pid) end)

      try do
        assert {:ok, :ok} = Task.yield(task, 1_000)
        assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
        assert Process.alive?(conn)
        :sys.resume(conn)
        await(fn -> Gnat.active_subscriptions(conn) == {:ok, 1} end)
        assert Process.whereis(:gnat) == conn
        refute_receive {:connected, ^pid, _}
      after
        :sys.resume(conn)
      end
    end
  end

  test "a subscriber exiting during retirement doesn't leave a stale monitor signal", %{
    stream: stream
  } do
    {pid, conn, _name} = pending_subscription(stream, :unsubscribe)
    ref = Process.monitor(pid)

    try do
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
      :sys.resume(conn)
      assert {:ok, 1} = Gnat.active_subscriptions(conn)
      assert Process.whereis(:gnat) == conn
    after
      if Process.alive?(conn), do: :sys.resume(conn)
    end
  end

  defp pending_subscription(stream, :subscribe) do
    conn = Process.whereis(:gnat)
    handler = pause_subscription(stream, conn)
    on_exit(fn -> :telemetry.detach(handler) end)
    pid = start_consumer(stream, :durable)
    assert_receive :subscription_paused
    await(fn -> pending_calls(conn, :sub) == 1 end)
    {pid, conn, "durable"}
  end

  defp pending_subscription(stream, :unsubscribe) do
    conn = Process.whereis(:gnat)
    pid = start_consumer(stream, :managed_durable)
    assert_receive {:connected, ^pid, name}

    # Wait for the initial pull to finish before suspending its connection.
    :sys.get_state(pid)
    :sys.suspend(conn)
    expire(pid)
    await(fn -> pending_calls(conn, :unsub) == 1 end)
    {pid, conn, name}
  end

  defp pending_subscription(stream, :connected) do
    conn = Process.whereis(:gnat)
    pid = start_consumer(stream, :managed_durable)
    assert_receive {:connected, ^pid, name}

    await(fn ->
      {:ok, info} = Consumer.info(conn, stream, name)
      info.num_waiting == 1
    end)

    :sys.suspend(conn)
    {pid, conn, name}
  end

  defp pause_subscription(stream, conn) do
    observer = self()
    handler = "pause-subscribe-#{stream}"
    topic = "$JS.API.CONSUMER.INFO.#{stream}.durable"
    paused = :atomics.new(1, [])

    :ok =
      :telemetry.attach(
        handler,
        [:gnat, :request],
        fn _, _, metadata, _ ->
          if metadata.topic == topic and :atomics.compare_exchange(paused, 1, 0, 1) == :ok do
            :sys.suspend(conn)
            send(observer, :subscription_paused)
          end
        end,
        nil
      )

    handler
  end

  defp pending_calls(conn, operation) do
    {:messages, messages} = Process.info(conn, :messages)

    Enum.count(messages, fn
      {:"$gen_call", _, request} -> elem(request, 0) == operation
      _ -> false
    end)
  end

  defp start_consumer(stream, mode, opts \\ []) do
    {deliver_policy, opts} = Keyword.pop(opts, :deliver_policy, :all)
    {ack_policy, opts} = Keyword.pop(opts, :ack_policy, :explicit)

    definition = %Consumer{
      ack_policy: ack_policy,
      stream_name: stream,
      deliver_policy: deliver_policy,
      max_deliver: 1
    }

    consumer_opts =
      case mode do
        :ephemeral ->
          [consumer: definition]

        :managed_durable ->
          [consumer: %{definition | durable_name: "managed", inactive_threshold: 30_000_000_000}]

        :durable ->
          {:ok, _} = Consumer.create(:gnat, %{definition | durable_name: "durable"})
          [stream_name: stream, consumer_name: "durable"]
      end

    start_supervised!(
      {ControlledConsumer,
       Keyword.merge(
         [
           test_pid: self(),
           request_expires: 1_000_000_000,
           idle_heartbeat: 500_000_000,
           heartbeat_check_interval: 10_000,
           connection_retry_timeout: 20
         ] ++ consumer_opts,
         opts
       )},
      restart: :temporary
    )
  end

  defp publish(stream, body), do: assert({:ok, _} = Gnat.request(:gnat, stream, body))

  defp expire_state(pid) do
    :sys.replace_state(pid, fn state ->
      put_in(state.mod_state.last_response_at, System.monotonic_time(:millisecond) - 2_000)
    end)
  end

  defp expire(pid) do
    expire_state(pid)
    send(pid, :heartbeat_check)
  end

  defp await(fun, attempts \\ 100)
  defp await(fun, 0), do: assert(fun.())

  defp await(fun, attempts) do
    unless fun.() do
      Process.sleep(10)
      await(fun, attempts - 1)
    end
  end
end
