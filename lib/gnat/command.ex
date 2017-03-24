defmodule Gnat.Command do
  @newline "\r\n"
  @unsub "UNSUB"

  def build(:unsub, sid, []), do: [@unsub, " #{sid}", @newline]
  def build(:unsub, sid, [max_messages: max]), do: [@unsub, " #{sid}", " #{max}", @newline]
end
