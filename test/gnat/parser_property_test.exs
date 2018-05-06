defmodule Gnat.ParserPropertyTest do
  use ExUnit.Case, async: true
  use PropCheck
  import Gnat.Generators, only: [message: 0]
  alias Gnat.Parser
  @numtests (System.get_env("N") || "100") |> String.to_integer

  @tag :property
  property "can parse any message" do
    numtests(@numtests * 10, forall map <- message() do
      {_parser, [parsed]} = Parser.new() |> Parser.parse(map.binary)
      {:msg, parsed_subject, parsed_sid, parsed_reply_to, parsed_payload} = parsed
      parsed_payload == map.payload &&
      parsed_subject == map.subject &&
      parsed_sid == map.sid &&
      parsed_reply_to == map.reply_to
    end)
  end

  @tag :property
  property "can parse messages split into arbitrary chunks" do
    numtests(@numtests, forall messages <- list(message()) do
      frames = messages |> Enum.reduce("", fn(%{binary: b},acc) -> acc<>b end) |> random_chunk
      payloads = Enum.map(messages, fn(msg) -> msg.payload end)
      {parser, parsed} = Enum.reduce(frames, {Parser.new(), []}, fn(frame, {parser, acc}) ->
        {parser, parsed_messages} = Parser.parse(parser, frame)
        payloads = Enum.map(parsed_messages, &( elem(&1, 4) ))
        {parser, acc ++ payloads}
      end)

      parser.partial == "" &&
      parsed == payloads
    end)
  end

  def random_chunk(binary), do: random_chunk(binary, [])
  def random_chunk("", list), do: Enum.reverse(list)
  def random_chunk(binary, list) do
    size = :rand.uniform(128)
    remaining = byte_size(binary)
    case remaining <= size do
      true -> random_chunk("", [binary | list])
      false -> random_chunk(:binary.part(binary, size, remaining - size), [:binary.part(binary, 0, size) | list])
    end
  end
end
