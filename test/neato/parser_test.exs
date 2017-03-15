defmodule Neato.ParserTest do
  use ExUnit.Case, async: true
  alias Neato.Parser

  test "parsing a complete message" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("MSG topic 13 4\r\ntest\r\n")
    assert parser_state.partial == ""
    assert parsed_message == {:msg, "topic", 13, "test"}
  end
end
