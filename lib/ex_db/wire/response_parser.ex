defmodule ExDb.Wire.ResponseParser do
  @moduledoc """
  Parser for Postgres wire protocol responses.
  """

  alias ExDb.Wire.Messages.{RowDescription, DataRow, CommandComplete}
  alias ExDb.Wire.ErrorMessage

  @doc """
  Parse a complete response stream into individual messages.
  Returns a list of parsed message structs.
  """
  def parse_response(data) do
    parse_messages(data, [])
  end

  defp parse_messages(<<>>, acc), do: Enum.reverse(acc)

  defp parse_messages(data, acc) do
    case parse_message(data) do
      {:ok, message, rest} ->
        parse_messages(rest, [message | acc])

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_message(<<type, length::32, rest::binary>>) when length >= 4 do
    data_length = length - 4

    case rest do
      <<message_data::binary-size(data_length), remaining::binary>> ->
        case parse_message_by_type(type, message_data) do
          {:ok, message} -> {:ok, message, remaining}
          {:error, reason} -> {:error, reason}
        end

      _ ->
        {:error, :incomplete_message}
    end
  end

  defp parse_message(_), do: {:error, :invalid_message_format}

  defp parse_message_by_type(?T, data) do
    case RowDescription.parse(<<"T", byte_size(data) + 4::32, data::binary>>) do
      %RowDescription{} = row_desc -> {:ok, row_desc}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_message_by_type(?D, data) do
    case DataRow.parse(<<"D", byte_size(data) + 4::32, data::binary>>) do
      %DataRow{} = data_row -> {:ok, data_row}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_message_by_type(?C, data) do
    case CommandComplete.parse(<<"C", byte_size(data) + 4::32, data::binary>>) do
      %CommandComplete{} = cmd_complete -> {:ok, cmd_complete}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_message_by_type(?Z, _data) do
    # ReadyForQuery - simple message, just return the type
    {:ok, :ready_for_query}
  end

  defp parse_message_by_type(?E, data) do
    case ErrorMessage.parse(<<"E", byte_size(data) + 4::32, data::binary>>) do
      %ErrorMessage{} = error_msg -> {:ok, error_msg}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_message_by_type(type, _data) do
    {:error, {:unknown_message_type, type}}
  end

  @doc """
  Extract specific message types from a response.
  """
  def extract_messages(response, type) when is_atom(type) do
    case parse_response(response) do
      {:ok, messages} ->
        Enum.filter(messages, &message_type?(&1, type))

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp message_type?(%RowDescription{}, :row_description), do: true
  defp message_type?(%DataRow{}, :data_row), do: true
  defp message_type?(%CommandComplete{}, :command_complete), do: true
  defp message_type?(%ErrorMessage{}, :error_response), do: true
  defp message_type?(:ready_for_query, :ready_for_query), do: true
  defp message_type?(_, _), do: false
end
