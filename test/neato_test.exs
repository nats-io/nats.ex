defmodule NeatoTest do
  use ExUnit.Case
  doctest Neato

  test "connect to a server" do
    {:ok, pid} = Neato.start_link()
    assert Process.alive?(pid)
    :ok = Neato.stop(pid)
  end
end
