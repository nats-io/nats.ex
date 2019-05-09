defmodule Gnat.Streaming.SubscriptionTest do
  use ExUnit.Case, async: true
  alias Gnat.Streaming.Subscription

  @subscription_response Gnat.Streaming.Protocol.SubscriptionResponse.new(
    ackInbox: "pawnee.swansonLane"
  )

  describe "disconnected state" do
    test "it starts in a disconnected state and tries to get connected" do
      assert {:ok, :disconnected, state, actions} = Subscription.init(client_name: :wat, subject: "eagleton.parks")
      assert actions == [{:next_event, :internal, :connect}]
      assert state.client_id == nil
      assert state.connection_pid == nil
      assert state.client_name == :wat
      assert state.subject == "eagleton.parks"
    end

    test "failed connect attempts keep the state the same and retries connection" do
      {:ok, :disconnected, state, _actions} = Subscription.init(client_name: :wat, subject: "eagleton.parks")
      assert {:keep_state_and_data, actions} = Subscription.disconnected(:internal, {:client_info, {:error, :disconnected}}, state)
      assert actions == [{{:timeout, :reconnect}, 250, :reconnect}]
    end

    test "finding the connection moves to connected status, tries to register client" do
      {:ok, :disconnected, state, _actions} = Subscription.init(client_name: :wat, subject: "eagleton.parks")
      assert {:next_state, :connected, state, actions} = Subscription.disconnected(:internal, {:client_info, {:ok, {"client_id", "sub_subject", self()}}}, state)
      assert state.client_id == "client_id"
      assert state.connection_pid == self()
      assert state.inbox == "client_id.eagleton.parks.INBOX"
      assert state.sub_subject == "sub_subject"
      assert actions == [{:next_event, :internal, :monitor_and_listen}]
    end
  end

  describe "connected state" do
    setup do
      {:ok, :disconnected, state, _actions} = Subscription.init(client_name: :wat, subject: "eagleton.parks")
      {:next_state, :connected, state, _actions} = Subscription.disconnected(:internal, {:client_info, {:ok, {"client_id", "sub_subject", self()}}}, state)
      {:ok, %{state: state}}
    end

    test "succesful subscription_response moves to subscribed state", %{state: state} do
      assert {:next_state, :subscribed, state, actions} = Subscription.connected(:internal, {:subscription_response, @subscription_response}, state)
      assert state.ack_subject == @subscription_response.ackInbox
      assert actions == []
    end

    @tag capture_log: true
    test "failed subscription_response does a delayed re-register", %{state: state} do
      response = Map.put(@subscription_response, :error, "500 unknown error")
      assert {:keep_state_and_data, actions} = Subscription.connected(:internal, {:subscription_response, response}, state)
      assert actions == [{{:timeout, :resubscribe}, 1_000, :resubscribe}]
    end

    test "connection dying pushes back to disconnected status", %{state: state} do
      down_message = {:DOWN, make_ref(), :process, state.connection_pid, :malnurished}
      assert {:next_state, :disconnected, state, actions} = Subscription.connected(:info, down_message, state)
      assert state.client_id == nil
      assert state.connection_pid == nil
      assert state.inbox == nil
      assert state.sub_subject == nil
      assert actions = [{{:timeout, :reconnect}, 250, :reconnect}]
    end
  end

  describe "subscribed state" do
    setup do
      {:ok, :disconnected, state, _actions} = Subscription.init(client_name: :wat, subject: "eagleton.parks")
      {:next_state, :connected, state, _actions} = Subscription.disconnected(:internal, {:client_info, {:ok, {"client_id", "sub_subject", self()}}}, state)
      {:next_state, :subscribed, state, _actions} = Subscription.connected(:internal, {:subscription_response, @subscription_response}, state)
      {:ok, %{state: state}}
    end

    test "connection dying pushes back to disconnected status", %{state: state} do
      down_message = {:DOWN, make_ref(), :process, state.connection_pid, :malnurished}
      assert {:next_state, :disconnected, state, actions} = Subscription.subscribed(:info, down_message, state)
      assert state.ack_subject == nil
      assert state.client_id == nil
      assert state.connection_pid == nil
      assert state.inbox == nil
      assert actions = [{{:timeout, :reconnect}, 250, :reconnect}]
    end
  end
end
