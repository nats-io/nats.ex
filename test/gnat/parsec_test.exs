defmodule Gnat.ParsecTest do
  use ExUnit.Case, async: true
  alias Gnat.Parsec

  test "parsing a complete message" do
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("MSG topic 13 4\r\ntest\r\n")
    assert parser_state.partial == nil
    assert parsed_message == {:msg, "topic", 13, nil, "test"}
  end

  test "parsing a complete message with newlines in it" do
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("MSG topic 13 10\r\ntest\r\nline\r\n")
    assert parser_state.partial == nil
    assert parsed_message == {:msg, "topic", 13, nil, "test\r\nline"}
  end

  test "parsing multiple messages" do
    {parser_state, [msg1,msg2]} = Parsec.new |> Parsec.parse("MSG t1 1 3\r\nwat\r\nMSG t2 2 4\r\ndawg\r\n")
    assert parser_state.partial == nil
    assert msg1 == {:msg, "t1", 1, nil, "wat"}
    assert msg2 == {:msg, "t2", 2, nil, "dawg"}
  end

  test "parsing a message with a reply to" do
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("MSG topic 13 me 10\r\ntest\r\nline\r\n")
    assert parser_state.partial == nil
    assert parsed_message == {:msg, "topic", 13, "me", "test\r\nline"}
  end

  test "handling _INBOX subjects" do
    inbox = "_INBOX.Rf+MI+V1+9pUCgC+.BChhlI06WHyCTYor"
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("MSG topic 13 #{inbox} 10\r\ntest\r\nline\r\n")
    assert parser_state.partial == nil
    assert parsed_message == {:msg, "topic", 13, inbox, "test\r\nline"}
  end

  test "parsing messages with headers - single header no reply" do
    binary = "HMSG SUBJECT 1 23 30\r\nNATS/1.0\r\nHeader: X\r\n\r\nPAYLOAD\r\n"

    {state, [parsed]} = Parsec.new() |> Parsec.parse(binary)
    assert state.partial == nil
    assert parsed == {:hmsg, "SUBJECT", 1, nil, [{"header", "X"}], "PAYLOAD"}
  end

  test "parsing messages with headers - single header" do
    binary = "HMSG SUBJECT 1 REPLY 23 30\r\nNATS/1.0\r\nHeader: X\r\n\r\nPAYLOAD\r\n"

    {state, [parsed]} = Parsec.new() |> Parsec.parse(binary)
    assert state.partial == nil
    assert parsed == {:hmsg, "SUBJECT", 1, "REPLY",[{"header", "X"}], "PAYLOAD"}
  end

  test "parsing PING message" do
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("PING\r\n")
    assert parser_state.partial == nil
    assert parsed_message == :ping
  end

  test "parsing a complete message with case insensitive command" do
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("msg topic 13 4\r\ntest\r\n")
    assert parser_state.partial == nil
    assert parsed_message == {:msg, "topic", 13, nil, "test"}
  end

  test "parsing case insensitive ping message" do
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("ping\r\n")
    assert parser_state.partial == nil
    assert parsed_message == :ping
  end

  test "parsing partial messages" do
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("PING\r\nMSG topic 11 4\r\nOH")
    assert parsed_message == :ping
    assert parser_state.partial == "MSG topic 11 4\r\nOH"
    {parser_state, [msg1,msg2]} = Parsec.parse(parser_state, "AI\r\nMSG topic 11 3\r\nWAT\r\nMSG topic")
    assert msg1 == {:msg, "topic", 11, nil, "OHAI"}
    assert msg2 == {:msg, "topic", 11, nil, "WAT"}
    assert parser_state.partial == "MSG topic"
  end

  test "parsing INFO message" do
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("INFO {\"server_id\":\"1ec445b504f4edfb4cf7927c707dd717\",\"version\":\"0.6.6\",\"go\":\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"max_payload\":1048576}\r\n")
    assert parser_state.partial == nil
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
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("PONG\r\n")
    assert parser_state.partial == nil
    assert parsed_message == :pong
  end

  test "parsing -ERR message" do
    {parser_state, [parsed_message]} = Parsec.new |> Parsec.parse("-ERR 'Unknown Protocol Operation'\r\n")
    assert parser_state.partial == nil
    assert parsed_message == {:error, "Unknown Protocol Operation"}
  end

  test "parsing -ERR messages in the middle of other traffic" do
    assert {parser, [:ping]} = Parsec.new |> Parsec.parse("PING\r\n-ERR 'Unknown Pro")
    assert {_, [{:error, "Unknown Protocol Operation"}]} = parser |> Parsec.parse("tocol Operation'\r\nMSG")
  end
end
