defmodule Gnat.ParserTest do
  use ExUnit.Case, async: true
  alias Gnat.Parser

  test "parsing a complete message" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("MSG topic 13 4\r\ntest\r\n")
    assert parser_state.partial == ""
    assert parsed_message == {:msg, "topic", 13, nil, "test"}
  end

  test "parsing a complete message with newlines in it" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("MSG topic 13 10\r\ntest\r\nline\r\n")
    assert parser_state.partial == ""
    assert parsed_message == {:msg, "topic", 13, nil, "test\r\nline"}
  end

  test "parsing multiple messages" do
    {parser_state, [msg1,msg2]} = Parser.new |> Parser.parse("MSG t1 1 3\r\nwat\r\nMSG t2 2 4\r\ndawg\r\n")
    assert parser_state.partial == ""
    assert msg1 == {:msg, "t1", 1, nil, "wat"}
    assert msg2 == {:msg, "t2", 2, nil, "dawg"}
  end

  test "parsing a message with a reply to" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("MSG topic 13 me 10\r\ntest\r\nline\r\n")
    assert parser_state.partial == ""
    assert parsed_message == {:msg, "topic", 13, "me", "test\r\nline"}
  end

  test "parsing PING message" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("PING\r\n")
    assert parser_state.partial == ""
    assert parsed_message == :ping
  end

  test "parsing a complete message with case insensitive command" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("msg topic 13 4\r\ntest\r\n")
    assert parser_state.partial == ""
    assert parsed_message == {:msg, "topic", 13, nil, "test"}
  end

  test "parsing case insensitive ping message" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("ping\r\n")
    assert parser_state.partial == ""
    assert parsed_message == :ping
  end

  test "parsing partial messages" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("PING\r\nMSG topic 11 4\r\nOH")
    assert parsed_message == :ping
    assert parser_state.partial == "MSG topic 11 4\r\nOH"
    {parser_state, [msg1,msg2]} = Parser.parse(parser_state, "AI\r\nMSG topic 11 3\r\nWAT\r\nMSG topic")
    assert msg1 == {:msg, "topic", 11, nil, "OHAI"}
    assert msg2 == {:msg, "topic", 11, nil, "WAT"}
    assert parser_state.partial == "MSG topic"
  end

  test "parsing INFO message" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("INFO {\"server_id\":\"1ec445b504f4edfb4cf7927c707dd717\",\"version\":\"0.6.6\",\"go\":\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"max_payload\":1048576}\r\n")
    assert parser_state.partial == ""
    assert parsed_message == {:info, %{
                                        server_id: "1ec445b504f4edfb4cf7927c707dd717",
                                        version: "0.6.6",
                                        go: "go1.4.2",
                                        host: "0.0.0.0",
                                        port: 4222,
                                        auth_required: false,
                                        ssl_required: false,
                                        max_payload: 1048576
                                      }}
  end

  test "parsing PONG message" do
    {parser_state, [parsed_message]} = Parser.new |> Parser.parse("PONG\r\n")
    assert parser_state.partial == ""
    assert parsed_message == :pong
  end
end