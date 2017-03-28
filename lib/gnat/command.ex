defmodule Gnat.Command do
  @newline "\r\n"
  @pub "PUB"
  @sub "SUB"
  @unsub "UNSUB"

  def build(:pub, topic, payload, []), do: [@pub, " ", topic, " #{IO.iodata_length(payload)}", @newline, payload, @newline]
  def build(:pub, topic, payload, [reply_to: reply]), do: [@pub, " ", topic, " ", reply, " #{IO.iodata_length(payload)}", @newline, payload, @newline]

  def build(:sub, topic, sid, []), do: [@sub, " ", topic, " ", Integer.to_string(sid), @newline]
  def build(:sub, topic, sid, [queue_group: qg]), do: [@sub, " ", topic, " ", qg, " ", Integer.to_string(sid), @newline]

  def build(:unsub, sid, []), do: [@unsub, " #{sid}", @newline]
  def build(:unsub, sid, [max_messages: max]), do: [@unsub, " #{sid}", " #{max}", @newline]
end
