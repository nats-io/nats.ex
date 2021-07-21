defmodule Gnat.Command do
  @moduledoc false

  @newline "\r\n"
  @hpub "HPUB"
  @pub "PUB"
  @sub "SUB"
  @unsub "UNSUB"

  def build(:pub, topic, payload, []), do: [@pub, " ", topic, " #{IO.iodata_length(payload)}", @newline, payload, @newline]
  def build(:pub, topic, payload, [reply_to: reply]), do: [@pub, " ", topic, " ", reply, " #{IO.iodata_length(payload)}", @newline, payload, @newline]
  def build(:pub, topic, payload, [headers: headers]) do
    # it takes 10 bytes to add the nats header version line
    # and 2 more for the newline between headers and payload
    header_len = IO.iodata_length(headers) + 12
    total_len = IO.iodata_length(payload) + header_len
    [@hpub, " ", topic, " ", Integer.to_string(header_len), " ", Integer.to_string(total_len), "\r\nNATS/1.0\r\n", headers, @newline, payload, @newline]
  end
  def build(:pub, topic, payload, [headers: headers, reply_to: reply]) do
    # it takes 10 bytes to add the nats header version line
    # and 2 more for the newline between headers and payload
    header_len = IO.iodata_length(headers) + 12
    total_len = IO.iodata_length(payload) + header_len
    [@hpub, " ", topic, " ", reply, " ", Integer.to_string(header_len), " ", Integer.to_string(total_len), "\r\nNATS/1.0\r\n", headers, @newline, payload, @newline]
  end

  def build(:sub, topic, sid, []), do: [@sub, " ", topic, " ", Integer.to_string(sid), @newline]
  def build(:sub, topic, sid, [queue_group: qg]), do: [@sub, " ", topic, " ", qg, " ", Integer.to_string(sid), @newline]

  def build(:unsub, sid, []), do: [@unsub, " #{sid}", @newline]
  def build(:unsub, sid, [max_messages: max]), do: [@unsub, " #{sid}", " #{max}", @newline]
end
