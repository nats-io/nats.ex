defmodule Gnat.Streaming.ClientTest do
  use ExUnit.Case, async: true
  alias Gnat.Streaming.Client

  @connect_response Gnat.Streaming.Protocol.ConnectResponse.new(
    closeRequests: "test-cluster.abc123.close",
    pubPrefix: "test-cluster.abc123.pub",
    subRequests: "test-cluster.abc123.sub",
    unsubRequests: "test-cluster.abc123.unsub"
  )

  describe "disconnected state" do
    test "it starts in a disconnected state and tries to get connected" do
      assert {:ok, :disconnected, state, actions} = Client.init(connection_name: :wat)
      assert actions == [{:next_event, :internal, :connect}]
      assert state.client_id != nil
      assert state.conn_id != nil
      assert state.connection_name == :wat
      assert state.connection_pid == nil
      assert state.heartbeat_subject != nil
    end

    test "failed connect attempts keep the state the same and retries connection" do
      {:ok, :disconnected, state, _actions} = Client.init(connection_name: :wat)
      assert {:keep_state_and_data, actions} = Client.disconnected(:internal, {:find_connection, nil}, state)
      assert actions == [{:timeout, 250, :reconnect}]
    end

    test "finding the connection moves to connected status, tries to register client" do
      {:ok, :disconnected, state, _actions} = Client.init(connection_name: :wat)
      assert {:next_state, :connected, state, actions} = Client.disconnected(:internal, {:find_connection, self()}, state)
      assert state.connection_pid == self()
      assert actions == [{:next_event, :internal, :monitor_and_subscribe}]
    end
  end

  describe "connected state" do
    setup do
      {:ok, :disconnected, state, _actions} = Client.init(connection_name: :wat)
      {:next_state, :connected, state, _actions} = Client.disconnected(:internal, {:find_connection, self()}, state)
      {:ok, %{state: state}}
    end

    test "succesful connect_response messages move to registered state", %{state: state} do
      assert {:next_state, :registered, state, actions} = Client.connected(:internal, {:connect_response, @connect_response}, state)
      assert state.close_subject == @connect_response.closeRequests
      assert state.pub_subject == "#{@connect_response.pubPrefix}.#{state.client_id}.#{state.conn_id}"
      assert state.sub_subject == @connect_response.subRequests
      assert state.unsub_subject == @connect_response.unsubRequests
      assert actions == []
    end

    @tag capture_log: true
    test "failed connect_response does a delayed re-register", %{state: state} do
      response = Map.put(@connect_response, :error, "500 unknown error")
      assert {:keep_state_and_data, actions} = Client.connected(:internal, {:connect_response, response}, state)
      assert actions == [{:timeout, 1_000, :reregister}]
    end

    test "connection dying pushes back to disconnected status", %{state: state} do
      down_message = {:DOWN, make_ref(), :process, state.connection_pid, :malnurished}
      assert {:next_state, :disconnected, state, actions} = Client.connected(:info, down_message, state)
      assert state.connection_pid == nil
      assert actions = [{:timeout, 250, :reconnect}]
    end
  end

  describe "registered state" do
    setup do
      {:ok, :disconnected, state, _actions} = Client.init(connection_name: :wat)
      {:next_state, :connected, state, _actions} = Client.disconnected(:internal, {:find_connection, self()}, state)
      {:next_state, :registered, state, _actions} = Client.connected(:internal, {:connect_response, @connect_response}, state)
      {:ok, %{state: state}}
    end

    test "receiving heartbeats", %{state: state} do
      process_message = {:msg, %{body: "", reply_to: "send_a_heartbeat_here"}}
      assert {:keep_state_and_data, actions} = Client.registered(:info, process_message, state)
      assert actions == [{:next_event, :internal, {:pub, "send_a_heartbeat_here", ""}}]
    end

    test "calls for publishing info", %{state: state} do
      assert {:keep_state_and_data, actions} = Client.registered({:call, :from}, :pub_info, state)
      assert [{:reply, :from, {:ok, pub_info}}] = actions
      assert pub_info == {state.client_id, state.pub_subject, state.connection_pid}
    end

    test "connection dying pushes back to disconnected status", %{state: state} do
      down_message = {:DOWN, make_ref(), :process, state.connection_pid, :malnurished}
      assert {:next_state, :disconnected, state, actions} = Client.registered(:info, down_message, state)
      assert state.connection_pid == nil
      assert state.close_subject == nil
      assert state.pub_subject == nil
      assert state.sub_subject == nil
      assert state.unsub_subject == nil
      assert actions = [{:timeout, 250, :reconnect}]
    end
  end
end
