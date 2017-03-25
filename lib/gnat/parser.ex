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
  def parse(parser, bytes, parsed) do
    {index, 2} = :binary.match(bytes, "\r\n")
    {command, "\r\n"<>rest} = String.split_at(bytes, index)
    {message, rest} = parse_command(command, rest)
    parsed = [ message | parsed]
    parse(parser, rest, parsed)
  end

  defp parse_command(command_str, body) do
    [command | details] = String.split(command_str)
    cond do
      String.match?(command, ~r{msg}i) -> parse_msg(details, body)
      String.match?(command, ~r{ping}i) -> {:ping, ""}
    end
  end

  defp parse_msg([topic, sidstr, sizestr], body), do: parse_msg([topic, sidstr, nil, sizestr], body)
  defp parse_msg([topic, sidstr, reply_to, sizestr], body) do
    sid = String.to_integer(sidstr)
    bytesize = String.to_integer(sizestr)
    << message :: binary-size(bytesize), "\r\n", rest :: binary >> = body
    {{:msg, topic, sid, reply_to, message}, rest}
  end
end
