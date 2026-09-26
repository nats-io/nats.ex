defmodule Gnat.Redaction do
  @moduledoc false

  # TLS options can contain private keys, passwords and callback state.
  @credential_keys [:password, :token, :jwt, :nkey_seed, :ssl_opts]

  def redact(term) when is_map(term) do
    :maps.map(fn key, value -> redact_value(key, value) end, term)
  end

  def redact({key, _value}) when key in @credential_keys, do: {key, :redacted}

  def redact(term) when is_tuple(term) do
    term |> Tuple.to_list() |> redact() |> List.to_tuple()
  end

  def redact([head | tail]), do: [redact(head) | redact(tail)]
  def redact(term), do: term

  def reraise_connection_error(exception, stacktrace) do
    message = "connection initialization raised #{inspect(exception.__struct__)}: [REDACTED]"
    reraise RuntimeError, [message: message], redact_stacktrace(stacktrace)
  end

  defp redact_stacktrace(stacktrace) do
    Enum.map(stacktrace, fn {module, function, args_or_arity, location} ->
      arity = if is_list(args_or_arity), do: length(args_or_arity), else: args_or_arity
      {module, function, arity, Keyword.take(location, [:file, :line])}
    end)
  end

  defp redact_value(key, _value) when key in @credential_keys, do: :redacted
  defp redact_value(_key, value), do: redact(value)
end
