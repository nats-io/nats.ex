defmodule Gnat.Parsec do
  @moduledoc false
  defstruct partial: nil, pending: nil

  import NimbleParsec

  subject = ascii_string([?!..?~], min: 1)
  length = integer(min: 1)
  sid = integer(min: 1)

  whitespace =
    ascii_char([32, ?\t])
    |> times(min: 1)

  op_msg =
    ascii_char([?m, ?M])
    |> ascii_char([?s, ?S])
    |> ascii_char([?g, ?G])

  op_hmsg =
    ascii_char([?h, ?H])
    |> ascii_char([?m, ?M])
    |> ascii_char([?s, ?S])
    |> ascii_char([?g, ?G])

  op_err =
    ascii_char([?-])
    |> ascii_char([?e, ?E])
    |> ascii_char([?r, ?R])
    |> ascii_char([?r, ?R])

  op_info =
    ascii_char([?i, ?I])
    |> ascii_char([?n, ?N])
    |> ascii_char([?f, ?F])
    |> ascii_char([?o, ?O])

  op_ping =
    ascii_char([?p, ?P])
    |> ascii_char([?i, ?I])
    |> ascii_char([?n, ?N])
    |> ascii_char([?g, ?G])

  op_pong =
    ascii_char([?p, ?P])
    |> ascii_char([?o, ?O])
    |> ascii_char([?n, ?N])
    |> ascii_char([?g, ?G])

  op_ok =
    ascii_char([?+])
    |> ascii_char([?o, ?O])
    |> ascii_char([?k, ?K])

  err =
    replace(op_err, :err)
    |> ignore(whitespace)
    |> ignore(string("'"))
    |> optional(utf8_string([not: ?'], min: 1))
    |> ignore(string("'\r\n"))

  msg =
    replace(op_msg, :msg)
    |> ignore(whitespace)
    |> concat(subject)
    |> ignore(whitespace)
    |> concat(sid)
    |> ignore(whitespace)
    |> choice([
      subject |> ignore(whitespace) |> concat(length),
      length
    ])
    |> ignore(string("\r\n"))

  hmsg =
    replace(op_hmsg, :hmsg)
    |> ignore(whitespace)
    |> concat(subject)
    |> ignore(whitespace)
    |> concat(sid)
    |> ignore(whitespace)
    |> choice([
      subject |> ignore(whitespace) |> concat(length) |> ignore(whitespace) |> concat(length),
      length |> ignore(whitespace) |> concat(length)
    ])
    |> ignore(string("\r\n"))

  ok = replace(op_ok |> string("\r\n"), :ok)
  ping = replace(op_ping |> string("\r\n"), :ping)
  pong = replace(op_pong |> string("\r\n"), :pong)

  info =
    replace(op_info, :info)
    |> ignore(whitespace)
    |> utf8_string([not: ?\r], min: 2)
    |> ignore(string("\r\n"))

  defparsecp(:command, choice([msg, hmsg, ok, ping, pong, info, err]))

  def new, do: %__MODULE__{}

  # Hot path: no partial header fragment, no pending body. This is the common case for
  # small messages that arrive complete in a single TCP chunk.
  def parse(%__MODULE__{partial: nil, pending: nil} = state, string) do
    {partial, pending, commands} = parse_commands(string, [])
    {%{state | partial: partial, pending: pending}, commands}
  end

  # Hot path variant: we have a partial header fragment from the previous chunk.
  # Prepend it and re-parse. Only the header is small, so the binary concatenation is cheap.
  def parse(%__MODULE__{partial: partial, pending: nil} = state, string) do
    {new_partial, pending, commands} = parse_commands(partial <> string, [])
    {%{state | partial: new_partial, pending: pending}, commands}
  end

  # Large-payload path: we already parsed the MSG header and are waiting for body bytes.
  # Accumulate incoming chunks into an iolist and only call iolist_to_binary once we have
  # enough bytes. This avoids the O(n²) cost of `partial <> string` that the original code
  # incurred — copying the entire accumulated buffer on every TCP chunk.
  def parse(
        %__MODULE__{pending: {:msg, subject, sid, reply_to, length, chunks, have}} = state,
        string
      ) do
    new_have = have + byte_size(string)
    new_chunks = [chunks, string]

    if new_have >= length + 2 do
      binary = :erlang.iolist_to_binary(new_chunks)
      <<body::binary-size(length), "\r\n", rest::binary>> = binary
      {new_partial, new_pending, more} = parse_commands(rest, [])

      {%{state | partial: new_partial, pending: new_pending},
       [{:msg, subject, sid, reply_to, body} | more]}
    else
      {%{state | pending: {:msg, subject, sid, reply_to, length, new_chunks, new_have}}, []}
    end
  end

  def parse(
        %__MODULE__{
          pending: {:hmsg, subject, sid, reply_to, header_length, total_length, chunks, have}
        } = state,
        string
      ) do
    new_have = have + byte_size(string)
    new_chunks = [chunks, string]

    if new_have >= total_length + 2 do
      payload_length = total_length - header_length
      binary = :erlang.iolist_to_binary(new_chunks)

      <<headers_bin::binary-size(header_length), payload::binary-size(payload_length),
        "\r\n", rest::binary>> = binary

      {:ok, status, description, headers} = parse_headers(headers_bin)
      {new_partial, new_pending, more} = parse_commands(rest, [])

      {%{state | partial: new_partial, pending: new_pending},
       [{:hmsg, subject, sid, reply_to, status, description, headers, payload} | more]}
    else
      {%{state | pending: {:hmsg, subject, sid, reply_to, header_length, total_length, new_chunks, new_have}}, []}
    end
  end

  def parse_commands("", list), do: {nil, nil, Enum.reverse(list)}

  def parse_commands(str, list) do
    case parse_command(str) do
      {:ok, command, rest} -> parse_commands(rest, [command | list])
      {:pending, pending_info} -> {nil, pending_info, Enum.reverse(list)}
      {:error, partial} -> {partial, nil, Enum.reverse(list)}
    end
  end

  @spec parse_command(binary()) ::
          {:ok, tuple(), binary()} | {:pending, tuple()} | {:error, binary()}
  def parse_command(string) do
    case command(string) do
      {:ok, [:msg, subject, sid, length], rest, _, _, _} ->
        finish_msg(subject, sid, nil, length, rest, string)

      {:ok, [:msg, subject, sid, reply_to, length], rest, _, _, _} ->
        finish_msg(subject, sid, reply_to, length, rest, string)

      {:ok, [:hmsg, subject, sid, header_length, total_length], rest, _, _, _} ->
        finish_hmsg(subject, sid, nil, header_length, total_length, rest, string)

      {:ok, [:hmsg, subject, sid, reply_to, header_length, total_length], rest, _, _, _} ->
        finish_hmsg(subject, sid, reply_to, header_length, total_length, rest, string)

      {:ok, [atom], rest, _, _, _} ->
        {:ok, atom, rest}

      {:ok, [:info, json], rest, _, _, _} ->
        {:ok, {:info, Jason.decode!(json, keys: :atoms)}, rest}

      {:ok, [:err, msg], rest, _, _, _} ->
        {:ok, {:error, msg}, rest}

      {:error, _, _, _, _, _} ->
        {:error, string}
    end
  end

  def parse_headers("NATS/1.0" <> rest) do
    case String.split(rest, "\r\n", parts: 2) do
      [" " <> status, headers] ->
        case :cow_http.parse_headers(headers) do
          {parsed, ""} ->
            case String.split(status, " ", parts: 2) do
              [status, description] ->
                {:ok, status, description, parsed}

              [status] ->
                {:ok, status, nil, parsed}
            end

          _other ->
            {:error, "Could not parse headers"}
        end

      [_status_line, headers] ->
        case :cow_http.parse_headers(headers) do
          {parsed, ""} -> {:ok, nil, nil, parsed}
          _other -> {:error, "Could not parse headers"}
        end

      _other ->
        {:error, "Could not parse status line"}
    end
  end

  def parse_headers(_other) do
    {:error, "Could not parse status line prefix"}
  end

  def finish_msg(subject, sid, reply_to, length, rest, _string) do
    case rest do
      <<body::size(length)-binary, "\r\n", rest::binary>> ->
        {:ok, {:msg, subject, sid, reply_to, body}, rest}

      _other ->
        # Header parsed, but body bytes not all here yet.
        # Return the partial body bytes already received so the caller can accumulate
        # subsequent chunks in an iolist without re-copying the growing buffer.
        {:pending, {:msg, subject, sid, reply_to, length, rest, byte_size(rest)}}
    end
  end

  def finish_hmsg(subject, sid, reply_to, header_length, total_length, rest, _string) do
    payload_length = total_length - header_length

    case rest do
      <<headers::size(header_length)-binary, payload::size(payload_length)-binary, "\r\n",
        rest::binary>> ->
        {:ok, status, description, headers} = parse_headers(headers)
        {:ok, {:hmsg, subject, sid, reply_to, status, description, headers, payload}, rest}

      _other ->
        {:pending,
         {:hmsg, subject, sid, reply_to, header_length, total_length, rest, byte_size(rest)}}
    end
  end
end
