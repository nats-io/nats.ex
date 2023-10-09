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

  test "microservice endpoint add works" do
    assert {:ok, %{body: "6"}} = Gnat.request(:test_connection, "calc.add", "foo")
  end

  test "microservice endpoint sub works" do
    assert {:ok, %{body: "4"}} = Gnat.request(:test_connection, "calc.sub", "foo")
  end

  test "microservice endpoint errors properly" do
    assert {:ok, %{body: "500 error"}} = Gnat.request(:test_connection, "calc.sub", "error")
  end

  test "microservice endpoint counters working" do
    # at least 1 error, at least 1 request, non-zero processing time
    assert {:ok, %{body: "4"}} = Gnat.request(:test_connection, "calc.sub", "foo")
    assert {:ok, %{body: "6"}} = Gnat.request(:test_connection, "calc.add", "foo")
    assert {:ok, %{body: "500 error"}} = Gnat.request(:test_connection, "calc.sub", "error")

    {:ok, %{body: body}} = Gnat.request(:test_connection, "$SRV.STATS.exampleservice", "")
    payload = Jason.decode!(body, keys: :atoms)
    assert Enum.at(payload.endpoints, 0) |> Map.get(:processing_time) > 1000
    assert Enum.at(payload.endpoints, 0) |> Map.get(:num_requests) > 0

    assert Enum.at(payload.endpoints, 1) |> Map.get(:processing_time) > 1000
    assert Enum.at(payload.endpoints, 1) |> Map.get(:num_requests) > 0

  end
end
