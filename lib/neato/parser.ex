defmodule Neato.Parser do
  defstruct [partial: ""]

  def new, do: %Neato.Parser{}

  def parse(parser, "MSG "<>rest) do
    [cmd | msg_chunks] = String.split(rest, "\r\n")
    [topic, sid_str, size_str] = String.split(cmd, " ")
    msg = Enum.join(msg_chunks, "\r\n") |> String.strip
    sid = String.to_integer(sid_str)
    {parser, [{:msg, topic, sid, msg}]}
  end
end
