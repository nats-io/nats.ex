defmodule Gnat.Jetstream.API.Object.Meta do
  @enforce_keys [:bucket, :chunks, :digest, :name, :nuid, :size]
  defstruct bucket: nil,
            chunks: nil,
            deleted: false,
            digest: nil,
            name: nil,
            nuid: nil,
            size: nil

  @type t :: %__MODULE__{
          bucket: String.t(),
          chunks: non_neg_integer(),
          deleted: boolean(),
          digest: String.t(),
          name: String.t(),
          nuid: String.t(),
          size: non_neg_integer()
        }
end

defimpl Jason.Encoder, for: Gnat.Jetstream.API.Object.Meta do
  alias Gnat.Jetstream.API.Object.Meta

  def encode(%Meta{deleted: true} = meta, opts) do
    Map.take(meta, [:bucket, :chunks, :deleted, :digest, :name, :nuid, :size])
    |> Jason.Encode.map(opts)
  end

  def encode(meta, opts) do
    Map.take(meta, [:bucket, :chunks, :digest, :name, :nuid, :size])
    |> Jason.Encode.map(opts)
  end
end
