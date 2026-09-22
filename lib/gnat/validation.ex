defmodule Gnat.Validation do
  @moduledoc false

  def subject!(subject, kind) do
    unless is_binary(subject) and valid_subject?(subject, kind, :start) do
      raise ArgumentError, "invalid #{kind} subject: #{inspect(subject)}"
    end

    :ok
  end

  def queue_group!(queue) do
    unless is_binary(queue) and queue != "" and valid_queue?(queue) do
      raise ArgumentError, "invalid queue group: #{inspect(queue)}"
    end

    :ok
  end

  def inbox_prefix!(prefix) do
    unless is_binary(prefix) and valid_subject?(prefix <> "inbox", :publish, :start) do
      raise ArgumentError, "invalid inbox prefix: #{inspect(prefix)}"
    end

    :ok
  end

  defp valid_subject?("", _kind, position), do: position == :token
  defp valid_subject?("*", :subscription, :start), do: true
  defp valid_subject?(">", :subscription, :start), do: true

  defp valid_subject?("*." <> rest, :subscription, :start),
    do: valid_subject?(rest, :subscription, :start)

  defp valid_subject?("." <> rest, kind, :token), do: valid_subject?(rest, kind, :start)

  defp valid_subject?(<<char::utf8, rest::binary>>, kind, _position)
       when char not in [?., ?*, ?>] do
    valid_character?(char) and valid_subject?(rest, kind, :token)
  end

  defp valid_subject?(_subject, _kind, _position), do: false

  defp valid_queue?(""), do: true

  defp valid_queue?(<<char::utf8, rest::binary>>),
    do: valid_character?(char) and valid_queue?(rest)

  defp valid_queue?(_queue), do: false

  defp valid_character?(char) do
    char > 32 and char not in 127..160 and
      char not in [0x1680, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000] and
      char not in 0x2000..0x200A
  end
end
