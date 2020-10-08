defmodule Gnat.ConsumerSupervisorTest do
  use ExUnit.Case, async: true

  # these requests are being handled by `ExampleServer` in the `test_helper.exs` file

  test "successful requests work fine" do
    assert {:ok, %{body: "Re: hi"}} = Gnat.request(:test_connection, "example.good", "hi")
  end

  test "catches returned errors" do
    assert {:ok, %{body: "400 error"}} = Gnat.request(:test_connection, "example.error", "hi")
  end

  test "catches raised errors" do
    assert {:ok, %{body: "500 error"}} = Gnat.request(:test_connection, "example.raise", "hi")
  end
end
