defmodule GnatPropertyTest do
  use ExUnit.Case, async: true
  use PropCheck
  import Gnat.Generators, only: [message: 0]
  @numtests (System.get_env("N") || "100") |> String.to_integer

  @tag :property
  property "can publish to random subjects" do
    numtests(@numtests * 2, forall %{subject: subject, payload: payload} <- message() do
      Gnat.pub(:test_connection, subject, payload) == :ok
    end)
  end

  @tag :property
  property "can subscribe, publish, receive and unsubscribe from subjects" do
    numtests(@numtests * 2, forall %{subject: subject, payload: payload} <- message() do
      {:ok, ref} = Gnat.sub(:test_connection, self(), subject)
      :ok = Gnat.pub(:test_connection, subject, payload)
      assert_receive {:msg, %{topic: ^subject, body: ^payload, reply_to: nil}}, 500
      Gnat.unsub(:test_connection, ref) == :ok
    end)
  end

  @tag :property
  property "can make requests to an echo endpoint" do
    numtests(@numtests * 2, forall %{subject: subject, payload: payload} <- message() do
      {:ok, msg} = Gnat.request(:test_connection, "rpc.#{subject}", payload)
      msg.body == payload
    end)
  end

  @tag :property
  property "auto-unsubscribes after n messages" do
    numtests(@numtests, forall {%{subject: subject, payload: payload}, max_messaages} <- {message(), pos_integer()} do
      {:ok, ref} = Gnat.sub(:test_connection, self(), subject)
      :ok = Gnat.unsub(:test_connection, ref, max_messages: max_messaages)
      Enum.each(1..max_messaages, fn(_) ->
        {:ok, 1} = Gnat.active_subscriptions(:test_connection)
        :ok = Gnat.pub(:test_connection, subject, payload)
        receive do
          {:msg, %{body: ^payload}} -> :ok
          after 100 -> raise "did not receive message from #{subject} within 100ms"
        end
      end)
      Gnat.active_subscriptions(:test_connection) == {:ok, 0}
    end)
  end
end
