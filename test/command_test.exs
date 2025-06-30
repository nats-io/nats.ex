defmodule Gnat.CommandTest do
  use ExUnit.Case, async: true
  alias Gnat.Command

  test "formatting a simple pub message" do
    command = Command.build(:pub, "topic", "payload", []) |> IO.iodata_to_binary()
    assert command == "PUB topic 7\r\npayload\r\n"
  end

  test "formatting a pub with reply_to set" do
    command = Command.build(:pub, "topic", "payload", reply_to: "INBOX") |> IO.iodata_to_binary()
    assert command == "PUB topic INBOX 7\r\npayload\r\n"
  end

  test "formatting a basic sub message" do
    command = Command.build(:sub, "foobar", 4, []) |> IO.iodata_to_binary()
    assert command == "SUB foobar 4\r\n"
  end

  test "formatting a sub with a queue group" do
    command = Command.build(:sub, "foobar", 5, queue_group: "us") |> IO.iodata_to_binary()
    assert command == "SUB foobar us 5\r\n"
  end

  test "formatting a simple unsub message" do
    command = Command.build(:unsub, 12, []) |> IO.iodata_to_binary()
    assert command == "UNSUB 12\r\n"
  end

  test "formatting an unsub message with max messages" do
    command = Command.build(:unsub, 12, max_messages: 3) |> IO.iodata_to_binary()
    assert command == "UNSUB 12 3\r\n"
  end
end
