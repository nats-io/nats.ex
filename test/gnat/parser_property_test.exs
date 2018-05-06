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
end
