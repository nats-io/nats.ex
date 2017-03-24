defmodule Gnat.CommandTest do
  use ExUnit.Case, async: true
  alias Gnat.Command

  test "formatting a simple unsub message" do
    command = Command.build(:unsub, 12, []) |> IO.iodata_to_binary
    assert command == "UNSUB 12\r\n"
  end

  test "formatting an unsub message with max messages" do
    command = Command.build(:unsub, 12, [max_messages: 3]) |> IO.iodata_to_binary
    assert command == "UNSUB 12 3\r\n"
  end
end
