defmodule Gnat.ParsecTest do
  use ExUnit.Case, async: true
  alias Gnat.Parsec

  test "parsing a complete message" do
    {parser_state, [parsed_message]} = Parsec.new() |> Parsec.parse("MSG topic 13 4\r\ntest\r\n")
    assert parser_state.partial == nil
    assert parsed_message == {:msg, "topic", 13, nil, "test"}
  end

  test "parsing a complete message with newlines in it" do
    {parser_state, [parsed_message]} =
      Parsec.new() |> Parsec.parse("MSG topic 13 10\r\ntest\r\nline\r\n")

    assert parser_state.partial == nil
    assert parsed_message == {:msg, "topic", 13, nil, "test\r\nline"}
  end

  test "parsing multiple messages" do
    {parser_state, [msg1, msg2]} =
      Parsec.new() |> Parsec.parse("MSG t1 1 3\r\nwat\r\nMSG t2 2 4\r\ndawg\r\n")

    assert parser_state.partial == nil
    assert msg1 == {:msg, "t1", 1, nil, "wat"}
    assert msg2 == {:msg, "t2", 2, nil, "dawg"}
  end

  test "parsing a message with a reply to" do
    {parser_state, [parsed_message]} =
      Parsec.new() |> Parsec.parse("MSG topic 13 me 10\r\ntest\r\nline\r\n")

    assert parser_state.partial == nil
    assert parsed_message == {:msg, "topic", 13, "me", "test\r\nline"}
  end

  test "handling _INBOX subjects" do
    inbox = "_INBOX.Rf+MI+V1+9pUCgC+.BChhlI06WHyCTYor"

    {parser_state, [parsed_message]} =
      Parsec.new() |> Parsec.parse("MSG topic 13 #{inbox} 10\r\ntest\r\nline\r\n")

    assert parser_state.partial == nil
    assert parsed_message == {:msg, "topic", 13, inbox, "test\r\nline"}
  end

  test "parsing messages with headers - single header no reply" do
    binary = "HMSG SUBJECT 1 23 30\r\nNATS/1.0\r\nHeader: X\r\n\r\nPAYLOAD\r\n"

    {state, [parsed]} = Parsec.new() |> Parsec.parse(binary)
    assert state.partial == nil
    assert parsed == {:hmsg, "SUBJECT", 1, nil, nil, nil, [{"header", "X"}], "PAYLOAD"}
  end

  test "parsing messages with headers - single header" do
    binary = "HMSG SUBJECT 1 REPLY 23 30\r\nNATS/1.0\r\nHeader: X\r\n\r\nPAYLOAD\r\n"

    {state, [parsed]} = Parsec.new() |> Parsec.parse(binary)
    assert state.partial == nil
    assert parsed == {:hmsg, "SUBJECT", 1, "REPLY", nil, nil, [{"header", "X"}], "PAYLOAD"}
  end

  # This example comes from https://github.com/nats-io/nats-architecture-and-design/blob/cb8f68af6ba730c00a6aa174dedaa217edd9edc6/adr/ADR-9.md
  test "parsing idle heartbeat messages" do
    binary =
      "HMSG my.messages 2  75 75\r\nNATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: 0\r\nNats-Last-Stream: 0\r\n\r\n\r\n"

    {state, [parsed]} = Parsec.new() |> Parsec.parse(binary)
    assert state.partial == nil

    assert parsed ==
             {:hmsg, "my.messages", 2, nil, "100", "Idle Heartbeat",
              [{"nats-last-consumer", "0"}, {"nats-last-stream", "0"}], ""}
  end

  # This example comes from https://github.com/nats-io/nats-architecture-and-design/blob/682d5cd5f21d18502da70025727128a407655250/adr/ADR-13.md
  test "parsing no wait pull request responses" do
    binary =
      "HMSG _INBOX.x7tkDPDLCOEknrfB4RH1V7.OgY4M7 2  28 28\r\nNATS/1.0 404 No Messages\r\n\r\n\r\n"

    {state, [parsed]} = Parsec.new() |> Parsec.parse(binary)
    assert state.partial == nil

    assert parsed ==
             {:hmsg, "_INBOX.x7tkDPDLCOEknrfB4RH1V7.OgY4M7", 2, nil, "404", "No Messages", [], ""}
  end

  test "parsing no_responders messages" do
    binary = "HMSG _INBOX.10ahfXw89Nx5htVf.7Il73yuah/RHW6w8 0 16 16\r\nNATS/1.0 503\r\n\r\n\r\n"

    {state, [parsed]} = Parsec.new() |> Parsec.parse(binary)
    assert state.partial == nil

    assert parsed ==
             {:hmsg, "_INBOX.10ahfXw89Nx5htVf.7Il73yuah/RHW6w8", 0, nil, "503", nil, [], ""}
  end

  test "parsing PING message" do
    {parser_state, [parsed_message]} = Parsec.new() |> Parsec.parse("PING\r\n")
    assert parser_state.partial == nil
    assert parsed_message == :ping
  end

  test "parsing a complete message with case insensitive command" do
    {parser_state, [parsed_message]} = Parsec.new() |> Parsec.parse("msg topic 13 4\r\ntest\r\n")
    assert parser_state.partial == nil
    assert parsed_message == {:msg, "topic", 13, nil, "test"}
  end

  test "parsing case insensitive ping message" do
    {parser_state, [parsed_message]} = Parsec.new() |> Parsec.parse("ping\r\n")
    assert parser_state.partial == nil
    assert parsed_message == :ping
  end

  test "parsing partial messages" do
    {parser_state, [parsed_message]} =
      Parsec.new() |> Parsec.parse("PING\r\nMSG topic 11 4\r\nOH")

    assert parsed_message == :ping
    assert parser_state.partial == "MSG topic 11 4\r\nOH"

    {parser_state, [msg1, msg2]} =
      Parsec.parse(parser_state, "AI\r\nMSG topic 11 3\r\nWAT\r\nMSG topic")

    assert msg1 == {:msg, "topic", 11, nil, "OHAI"}
    assert msg2 == {:msg, "topic", 11, nil, "WAT"}
    assert parser_state.partial == "MSG topic"
  end

  test "parsing INFO message" do
    {parser_state, [parsed_message]} =
      Parsec.new()
      |> Parsec.parse(
        "INFO {\"server_id\":\"1ec445b504f4edfb4cf7927c707dd717\",\"version\":\"0.6.6\",\"go\":\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"max_payload\":1048576}\r\n"
      )

    assert parser_state.partial == nil

    assert parsed_message ==
             {:info,
              %{
                server_id: "1ec445b504f4edfb4cf7927c707dd717",
                version: "0.6.6",
                go: "go1.4.2",
                host: "0.0.0.0",
                port: 4222,
                auth_required: false,
                ssl_required: false,
                max_payload: 1_048_576
              }}
  end

  test "parsing PONG message" do
    {parser_state, [parsed_message]} = Parsec.new() |> Parsec.parse("PONG\r\n")
    assert parser_state.partial == nil
    assert parsed_message == :pong
  end

  test "parsing -ERR message" do
    {parser_state, [parsed_message]} =
      Parsec.new() |> Parsec.parse("-ERR 'Unknown Protocol Operation'\r\n")

    assert parser_state.partial == nil
    assert parsed_message == {:error, "Unknown Protocol Operation"}
  end

  test "parsing -ERR messages in the middle of other traffic" do
    assert {parser, [:ping]} = Parsec.new() |> Parsec.parse("PING\r\n-ERR 'Unknown Pro")

    assert {_, [{:error, "Unknown Protocol Operation"}]} =
             parser |> Parsec.parse("tocol Operation'\r\nMSG")
  end
end
