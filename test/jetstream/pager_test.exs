defmodule Gnat.Jetstream.PagerTest do
  use Gnat.Jetstream.ConnCase
  alias Gnat.Jetstream.Pager
  alias Gnat.Jetstream.API.Stream

  @moduletag with_gnat: :gnat

  test "paging over a simple stream" do
    {:ok, _stream} = create_stream("pager_a")

    Enum.each(1..100, fn i ->
      :ok = Gnat.pub(:gnat, "input.pager_a", "#{i}")
    end)

    {:ok, res} =
      Pager.reduce(:gnat, "pager_a", [from_seq: 1], 0, fn msg, total ->
        total + String.to_integer(msg.body)
      end)

    assert res == 5050

    {:ok, res} =
      Pager.reduce(:gnat, "pager_a", [from_seq: 51], 0, fn msg, total ->
        total + String.to_integer(msg.body)
      end)

    assert res == 3775

    Stream.delete(:gnat, "pager_a")
  end

  defp create_stream(name) do
    stream = %Stream{
      name: name,
      subjects: ["input.#{name}"]
    }

    Stream.create(:gnat, stream)
  end
end
