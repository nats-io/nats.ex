defmodule Gnat.ConsumerSupervisorTest do
  alias Gnat.ConsumerSupervisor
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

  # The happy path is setup in `test_helper.exs`
  # check the ExampleService module for the implementation of the endpoints

  test "microservice endpoint add works" do
    assert {:ok, %{body: "6"}} = Gnat.request(:test_connection, "calc.add", "foo")
  end

  test "microservice endpoint sub works" do
    assert {:ok, %{body: "4"}} = Gnat.request(:test_connection, "calc.sub", "foo")
  end

  test "microservice endpoint errors properly" do
    assert {:ok, %{body: "500 error"}} = Gnat.request(:test_connection, "calc.sub", "error")
  end

  test "service endpoint ping response" do
    {:ok, %{body: body}} = Gnat.request(:test_connection, "$SRV.PING.exampleservice", "")
    payload = Jason.decode!(body, keys: :atoms)
    assert payload.name == "exampleservice"
    assert is_binary(payload.id)
    assert payload.version == "0.1.0"
    assert payload.metadata == %{}
  end

  test "services info response" do
    {:ok, %{body: body}} = Gnat.request(:test_connection, "$SRV.INFO.exampleservice", "")
    payload = Jason.decode!(body, keys: :atoms)
    assert payload.name == "exampleservice"
    assert is_binary(payload.id)
    assert payload.version == "0.1.0"
    assert payload.description == "This is an example service"
    assert payload.metadata == %{}

    [add, sub] = Enum.sort_by(payload.endpoints, & &1.name)
    assert add == %{
      name: "add",
      subject: "calc.add",
      queue_group: "q",
      metadata: %{}
    }
    assert sub == %{
      name: "sub",
      subject: "calc.sub",
      queue_group: "q",
      metadata: %{}
    }
  end

  test "service endpoint stats response" do
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

  test "validates the version of a service" do
    bad = %{
      name: "exampleservice",
      description: "This is an example service",
      version: "0.1",
      endpoints: [
        %{
          name: "add",
          group_name: "calc"
        }
      ]
    }

    assert {:error, message} = start_service_supervisor(bad)
    assert message =~ "Version '0.1' does not conform to semver specification"
  end

  test "validates the name of a service" do
    bad = %{
      name: "example!",
      description: "This is an example service",
      version: "0.1.0",
      endpoints: [
        %{
          name: "add",
          group_name: "calc",
        }
      ]
    }

    assert {:error, message} = start_service_supervisor(bad)
    assert message =~ "Service name 'example!' is invalid."
  end

  test "validates the name of an endpoint" do
    bad = %{
      name: "example",
      description: "This is an example service",
      version: "0.1.0",
      endpoints: [
        %{
          name: "add some stuff",
          group_name: "calc",
        }
      ]
    }

    assert {:error, message} = start_service_supervisor(bad)
    assert message =~ "Endpoint name 'add some stuff' is not valid"
  end

  test "validates metadata of a service" do
    bad = %{
      name: "exampleservice",
      description: "This is an example service",
      version: "0.0.1",
      endpoints: [
        %{
          name: "add",
          group_name: "calc",
          metadata: %{ :blarg => :thisisbad }
        }
      ]
    }

    assert {:error, message} = start_service_supervisor(bad)
    assert message =~ "At least one key or value found in metadata that was not a string"
  end

  # In OTP 26 the GenServer.init behavior changed to do a process EXIT when returning a
  # {:stop, error} in GenServer.init
  # We inherit this behavior, so for the purpose of testing, we trap those process exits
  # to make sure we can process the `{:error, error}` tuple before the process exit kills
  # our ExUnit test
  defp start_service_supervisor(service_config) do
    Process.flag(:trap_exit, true)

    config = %{
      connection_name: :something,
      module: ExampleService,
      service_definition: service_config
    }
    ConsumerSupervisor.start_link(config)
  end
end
