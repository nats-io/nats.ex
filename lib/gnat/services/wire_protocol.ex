defmodule Gnat.Services.WireProtocol do
  @moduledoc false

  defmodule InfoResponse do
    @moduledoc false
    @info_response_type "io.nats.micro.v1.info_response"

    @type endpoint :: %{
      name:       String.t,
      subject:    String.t,
      metadata:   map(),
      queue_group: String.t
    }

    @type t :: %__MODULE__{
      name:         String.t,
      id:           String.t,
      version:      String.t,
      description:  String.t,
      metadata:     map(),
      endpoints:    [endpoint()],
      type:         String.t
    }

    @derive Jason.Encoder
    @enforce_keys [:name, :id, :version]
    defstruct [
                :name,
                :id,
                :version,
                :metadata,
                :description,
                endpoints: [],
                type: @info_response_type
    ]

  end

  defmodule PingResponse do
    @moduledoc false
    @ping_response_type "io.nats.micro.v1.ping_response"

    @type t :: %__MODULE__{
      name:     String.t,
      id:       String.t,
      version:  String.t,
      metadata: map()
    }

    @derive Jason.Encoder
    @enforce_keys [:name, :id, :version]
    defstruct [
        :name,
        :id,
        :version,
        :metadata,
        type: @ping_response_type
    ]
  end

  defmodule StatsResponse do
    @moduledoc false
    @stats_response_type "io.nats.micro.v1.stats_response"

    @type endpoint :: %{
      name:                     String.t,
      subject:                  String.t,
      num_requests:             integer,
      num_errors:               integer,
      last_error:               String.t,
      processing_time:          integer,
      average_processing_time:  integer,
      queue_group:              String.t,
      data:                     map()
    }

    @type t :: %__MODULE__{
      name:       String.t,
      id:         String.t,
      version:    String.t,
      metadata:   map(),
      started:    String.t,
      endpoints:  [endpoint()],
      type:       String.t
    }

    @derive Jason.Encoder
    @enforce_keys [:name, :id]
    defstruct [
        :name,
        :id,
        :version,
        :metadata,
        :started,
        :endpoints,
        type: @stats_response_type
    ]
  end
end
