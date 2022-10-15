defmodule Gnat.Parsec do
  @moduledoc false
  defstruct partial: nil

  import NimbleParsec

  subject = ascii_string([?!..?~], min: 1)
  length = integer(min: 1)
  sid = integer(min: 1)
  whitespace = ascii_char([32, ?\t])
               |> times(min: 1)
  op_msg = ascii_char([?m,?M])
           |> ascii_char([?s,?S])
           |> ascii_char([?g,?G])
  op_hmsg = ascii_char([?h,?H])
           |> ascii_char([?m,?M])
           |> ascii_char([?s,?S])
           |> ascii_char([?g,?G])
  op_err = ascii_char([?-])
           |> ascii_char([?e,?E])
           |> ascii_char([?r,?R])
           |> ascii_char([?r,?R])
  op_info = ascii_char([?i,?I])
            |> ascii_char([?n,?N])
            |> ascii_char([?f,?F])
            |> ascii_char([?o,?O])
  op_ping = ascii_char([?p,?P])
            |> ascii_char([?i,?I])
            |> ascii_char([?n,?N])
            |> ascii_char([?g,?G])
  op_pong = ascii_char([?p,?P])
            |> ascii_char([?o,?O])
            |> ascii_char([?n,?N])
            |> ascii_char([?g,?G])
  op_ok = ascii_char([?+])
          |> ascii_char([?o,?O])
          |> ascii_char([?k,?K])
  err = replace(op_err, :err)
        |> ignore(whitespace)
        |> ignore(string("'"))
        |> optional(utf8_string([not: ?'], min: 1))
        |> ignore(string("'\r\n"))
  msg = replace(op_msg, :msg)
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
  hmsg = replace(op_hmsg, :hmsg)
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
  info = replace(op_info, :info)
         |> ignore(whitespace)
         |> utf8_string([not: ?\r], min: 2)
         |> ignore(string("\r\n"))

  defparsecp :command, choice([msg, hmsg, ok, ping, pong, info, err])

  def new, do: %__MODULE__{}

  def parse(%__MODULE__{partial: nil}=state, string) do
    {partial, commands} = parse_commands(string, [])
    {%{state | partial: partial}, commands}
  end
  def parse(%__MODULE__{partial: partial}=state, string) do
    {partial, commands} = parse_commands(partial <> string, [])
    {%{state | partial: partial}, commands}
  end

  def parse_commands("", list), do: {nil, Enum.reverse(list)}
  def parse_commands(str, list) do
    case parse_command(str) do
      {:ok, command, rest} -> parse_commands(rest, [command | list])
      {:error, partial} -> {partial, Enum.reverse(list)}
    end
  end

  @spec parse_command(binary()) :: {:ok, tuple(), binary()} | {:error, binary()}
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

  def finish_msg(subject, sid, reply_to, length, rest, string) do
    case rest do
      << body::size(length)-binary, "\r\n", rest::binary>> ->
        {:ok, {:msg, subject, sid, reply_to, body}, rest}
      _other ->
        {:error, string}
    end
  end

  def finish_hmsg(subject, sid, reply_to, header_length, total_length, rest, string) do
    payload_length = total_length - header_length
    case rest do
      << headers::size(header_length)-binary, payload::size(payload_length)-binary, "\r\n", rest::binary>> ->
        {:ok, status, description, headers} = parse_headers(headers)
        {:ok, {:hmsg, subject, sid, reply_to, status, description, headers, payload}, rest}
      _other ->
        {:error, string}
    end
  end
end
