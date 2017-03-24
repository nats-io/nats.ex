defmodule Gnat.Parser do
  require Logger
  # states: waiting, reading_message
  defstruct [
    partial: "",
    state: :waiting,
  ]

  def new, do: %Gnat.Parser{}

  def parse(parser, data) do
    parse(parser, data, [])
  end

  def parse(parser, "", parsed), do: {parser, Enum.reverse(parsed)}
  def parse(parser, "PING\r\n", _), do: {parser, [:ping]}
  def parse(parser, bytes, parsed) do
    {index, 2} = :binary.match(bytes, "\r\n")
    {command, "\r\n"<>rest} = String.split_at(bytes, index)
    ["MSG", topic, sidstr, sizestr] = String.split(command)
    bytesize = String.to_integer(sizestr)
    sid = String.to_integer(sidstr)
    << message :: binary-size(bytesize), "\r\n", rest :: binary >> = rest
    parsed = [ {:msg, topic, sid, message} | parsed]
    parse(parser, rest, parsed)
  end
end
