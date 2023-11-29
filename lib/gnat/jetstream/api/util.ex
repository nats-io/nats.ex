defmodule Gnat.Jetstream.API.Util do
  @moduledoc false

  @default_inbox_prefix "_INBOX."

  def request(conn, topic, payload) do
    with {:ok, %{body: body}} <- Gnat.request(conn, topic, payload),
         {:ok, decoded} <- Jason.decode(body) do
      case decoded do
        %{"error" => err} ->
          {:error, err}

        other ->
          {:ok, other}
      end
    end
  end

  def to_datetime(nil), do: nil

  def to_datetime(str) do
    {:ok, datetime, _} = DateTime.from_iso8601(str)
    datetime
  end

  def to_sym(nil), do: nil

  def to_sym(str) when is_binary(str) do
    String.to_existing_atom(str)
  end

  def put_if_exist(target_map, target_key, source_map, source_key) do
    case Map.fetch(source_map, source_key) do
      {:ok, value} -> Map.put(target_map, target_key, value)
      _ -> target_map
    end
  end

  def valid_name?(name) do
    !String.contains?(name, [".", "*", ">", " ", "\t"])
  end

  def invalid_name_message do
    "cannot contain '.', '>', '*', spaces or tabs"
  end

  def decode_base64(nil), do: nil
  def decode_base64(data), do: Base.decode64!(data)

  def reply_inbox(prefix \\ @default_inbox_prefix)
  def reply_inbox(nil), do: reply_inbox()
  def reply_inbox(prefix), do: prefix <> nuid()

  def nuid() do
    :crypto.strong_rand_bytes(12) |> Base.url_encode64()
  end
end
