defmodule Gnat.Validation do
  @moduledoc false

  def subject!(subject, kind) do
    unless subject != "" and valid_field?(subject) do
      raise ArgumentError, "invalid #{kind} subject: #{inspect(subject)}"
    end

    :ok
  end

  def queue_group!(queue) do
    unless valid_field?(queue) do
      raise ArgumentError, "invalid queue group: #{inspect(queue)}"
    end

    :ok
  end

  def inbox_prefix!(prefix) do
    unless valid_field?(prefix) do
      raise ArgumentError, "invalid inbox prefix: #{inspect(prefix)}"
    end

    :ok
  end

  defp valid_field?(value) when is_binary(value) do
    :binary.match(value, [" ", "\t", "\r", "\n"]) == :nomatch
  end

  defp valid_field?(_value), do: false
end
