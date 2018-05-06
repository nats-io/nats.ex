defmodule Gnat.Generators do
  use PropCheck

  def alphanumeric_char do
    elements('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
  end

  def delimiter, do: let(chunks <- non_empty(list(delimiter_char())), do: Enum.join(chunks, ""))

  def delimiter_char, do: union([" ","\t"])

  def error do
    let chars <- list(error_char) do
      %{binary: "-ERR '#{List.to_string(chars)}'\r\n"}
    end
  end

  def error_char do
    elements(' 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
  end

  def ok, do: %{binary: "+OK\r\n"}

  def ping, do: %{binary: "PING\r\n"}

  def pong, do: %{binary: "PONG\r\n"}

  # generates a map containing the binary encoded message and attributes for which generated
  # sid, subject, payload and reply_to topic were used in the encoded message
  def message, do: sized(size, message(size))
  def message(size), do: union([message_without_reply(size), message_with_reply(size)])
  def message_with_reply(size) do
    let(
      {p, su, si, r, d1, d2, d3, d4} <-
      {payload(size), subject(), sid(), reply_to(), delimiter(), delimiter(), delimiter(), delimiter()}
    ) do
      parts = ["MSG", d1, su, d2, si, d3, r, d4, byte_size(p), "\r\n", p, "\r\n"]
      %{
        binary: Enum.join(parts),
        reply_to: r,
        sid: si,
        subject: su,
        payload: p,
      }
    end
  end
  def message_without_reply(size) do
    let(
      {p, su, si, d1, d2, d3} <-
      {payload(size), subject(), sid(), delimiter(), delimiter(), delimiter()}
    ) do
      parts = ["MSG", d1, su, d2, si, d3, byte_size(p), "\r\n", p, "\r\n"]
      %{
        binary: Enum.join(parts),
        reply_to: nil,
        sid: si,
        subject: su,
        payload: p,
      }
    end
  end

  def payload(size), do: binary(size)

  def protocol_message do
    union([ok(), ping(), pong(), error(), message()])
  end

  # according to the spec sid's can be alphanumeric, but our client only generates
  # non-negative integers and we only receive back our own sids
  def sid, do: non_neg_integer()

  def subject do
    let(chunks <- subject_chunks(), do: Enum.join(chunks, "."))
  end
  def subject_chunks do
    non_empty(list(
      non_empty(list(
        alphanumeric_char()
      ))
    ))
  end

  # TODO subsription names are like subject names, but they can have wildcards
  # There is a special case where a user can subscribe to ">" to subscribe to all topics

  def reply_to, do: subject()
end
