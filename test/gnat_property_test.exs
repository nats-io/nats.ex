defmodule GnatPropertyTest do
  use ExUnit.Case, async: true
  use PropCheck
  import Gnat.Generators, only: [message: 0]
  @numtests (System.get_env("N") || "100") |> String.to_integer()

  @tag :property
  property "can publish to random subjects" do
    numtests(
      @numtests * 2,
      forall %{subject: subject, payload: payload} <- message() do
        Gnat.pub(:test_connection, subject, payload) == :ok
      end
    )
  end

  @tag :property
  property "can subscribe, publish, receive and unsubscribe from subjects" do
    numtests(
      @numtests * 2,
      forall %{subject: subject, payload: payload} <- message() do
        {:ok, ref} = Gnat.sub(:test_connection, self(), subject)
        :ok = Gnat.pub(:test_connection, subject, payload)
        assert_receive {:msg, %{topic: ^subject, body: ^payload, reply_to: nil}}, 500
        Gnat.unsub(:test_connection, ref) == :ok
      end
    )
  end

  @tag :property
  property "can make requests to an echo endpoint" do
    numtests(
      @numtests * 2,
      forall %{subject: subject, payload: payload} <- message() do
        {:ok, msg} = Gnat.request(:test_connection, "rpc.#{subject}", payload)
        msg.body == payload
      end
    )
  end
end
