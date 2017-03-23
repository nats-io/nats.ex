defmodule Gnat.ParserTest do
  use ExUnit.Case, async: true
  alias Gnat.Parser

  test "parsing a complete message" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("MSG topic 13 4\r\ntest\r\n")
    assert parser_state.partial == ""
    assert parsed_message == {:msg, "topic", 13, "test"}
  end

  test "parsing a complete message with newlines in it" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("MSG topic 13 10\r\ntest\r\nline\r\n")
    assert parser_state.partial == ""
    assert parsed_message == {:msg, "topic", 13, "test\r\nline"}
  end

  test "parsing multiple messages" do
    {parser_state, [msg1,msg2]} = Parser.new |> Parser.parse("MSG t1 1 3\r\nwat\r\nMSG t2 2 4\r\ndawg\r\n")
    assert parser_state.partial == ""
    assert msg1 == {:msg, "t1", 1, "wat"}
    assert msg2 == {:msg, "t2", 2, "dawg"}
  end
end
