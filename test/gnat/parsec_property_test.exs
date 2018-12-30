defmodule Gnat.ParsecPropertyTest do
  use ExUnit.Case, async: true
  use PropCheck
  import Gnat.Generators, only: [message: 0, protocol_message: 0]
  alias Gnat.Parsec
  @numtests (System.get_env("N") || "100") |> String.to_integer

  @tag :property
  property "can parse any message" do
    numtests(@numtests * 10, forall map <- message() do
      {_parser, [parsed]} = Parsec.new() |> Parsec.parse(map.binary)
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
      {parser, parsed} = Enum.reduce(frames, {Parsec.new(), []}, fn(frame, {parser, acc}) ->
        {parser, parsed_messages} = Parsec.parse(parser, frame)
        payloads = Enum.map(parsed_messages, &( elem(&1, 4) ))
        {parser, acc ++ payloads}
      end)

      parser.partial == nil &&
      parsed == payloads
    end)
  end

  @tag :property
  property "can parse any sequence of protocol messages without exception" do
    numtests(@numtests, forall messages <- list(protocol_message()) do
      parser = Enum.reduce(messages, Parsec.new(), fn(%{binary: bin}, parser) ->
        {parser, [_parsed]} = Parsec.parse(parser, bin)
        parser
      end)
      parser.partial == nil
    end)
  end

  @tag :property
  property "can parse any sequence of protocol messages, randomly chunked without exception" do
    numtests(@numtests, forall messages <- list(protocol_message()) do
      bin = Enum.reduce(messages, "", fn(%{binary: part}, acc) -> acc<>part end)
      chunks = random_chunk(bin)
      {parser, parsed} = Enum.reduce(chunks, {Parsec.new(), []}, fn(bin, {parser, acc}) ->
        {parser, parsed} = Parsec.parse(parser, bin)
        {parser, acc ++ parsed}
      end)
      parser.partial == nil &&
      Enum.count(parsed) == Enum.count(messages)
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
