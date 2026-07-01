defmodule Gnat.Headers do
  @moduledoc false

  @spec encode([{iodata(), iodata()}]) :: iodata()
  def encode(headers) do
    Enum.map(headers, fn {name, value} -> [name, ": ", value, "\r\n"] end)
  end

  @spec decode(binary()) :: {[{binary(), binary()}], binary()} | :error
  def decode(binary), do: decode(binary, [])

  defp decode(binary, acc) do
    case :erlang.decode_packet(:httph_bin, binary, []) do
      {:ok, {:http_header, _num, _field, raw_name, value}, rest} ->
        decode(rest, [{String.downcase(raw_name, :ascii), String.trim_trailing(value)} | acc])

      {:ok, :http_eoh, rest} ->
        {Enum.reverse(acc), rest}

      _other ->
        :error
    end
  end
end
