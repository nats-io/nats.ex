defmodule Gnat.Streaming.Message do
  @enforce_keys [:ack_subject, :connection_pid, :data, :redelivered, :reply, :sequence, :subject, :timestamp]
  defstruct [:ack_subject, :connection_pid, :data, :redelivered, :reply, :sequence, :subject, :timestamp]

  alias Gnat.Streaming.Protocol.Ack

  def ack(%__MODULE__{}=message) do
    %__MODULE__{
      ack_subject: ack_subject,
      connection_pid: pid,
      sequence: sequence,
      subject: subject} = message
    ack = Ack.new(subject: subject, sequence: sequence) |> Ack.encode()
    Gnat.pub(pid, ack_subject, ack)
  end
end
