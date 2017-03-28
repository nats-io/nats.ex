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

  defp parse_command(command, body) do
    [operation | details] = String.split(command)
    operation
    |> String.upcase
    |> parse_command(details, body)
  end

  defp parse_command("PING", _, body), do: {:ping, body}
  defp parse_command("MSG", [topic, sidstr, sizestr], body), do: parse_command("MSG", [topic, sidstr, nil, sizestr], body)
  defp parse_command("MSG", [topic, sidstr, reply_to, sizestr], body) do
    sid = String.to_integer(sidstr)
    bytesize = String.to_integer(sizestr)
    << message :: binary-size(bytesize), "\r\n", rest :: binary >> = body
    {{:msg, topic, sid, reply_to, message}, rest}
  end
end
