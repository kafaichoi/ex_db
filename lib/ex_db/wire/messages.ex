defmodule ExDb.Wire.Messages do
  @moduledoc """
  Postgres wire protocol message builders and parsers.
  """

  # Message structs for parsing responses
  defmodule RowDescription do
    defstruct [:field_count, :fields]

    def parse(data) do
      case data do
        <<"T", _length::32, field_count::16, rest::binary>> ->
          fields = parse_fields(rest, field_count, [])
          %__MODULE__{field_count: field_count, fields: fields}

        _ ->
          {:error, :invalid_row_description}
      end
    end

    defp parse_fields(<<>>, 0, acc), do: Enum.reverse(acc)

    defp parse_fields(data, count, acc) do
      case parse_field(data) do
        {field, rest} -> parse_fields(rest, count - 1, [field | acc])
        _ -> {:error, :invalid_field}
      end
    end

    defp parse_field(data) do
      case String.split(data, <<0>>, parts: 2) do
        [name, rest] ->
          case rest do
            <<table_oid::32, column_attr::16, type_oid::32, type_size::16, type_modifier::32,
              format_code::16, rest2::binary>> ->
              field = %{
                name: name,
                table_oid: table_oid,
                column_attr: column_attr,
                type_oid: type_oid,
                type_size: type_size,
                type_modifier: type_modifier,
                format_code: format_code
              }

              {field, rest2}

            _ ->
              {:error, :invalid_field_data}
          end

        _ ->
          {:error, :invalid_field_name}
      end
    end
  end

  defmodule DataRow do
    defstruct [:field_count, :fields]

    def parse(data) do
      case data do
        <<"D", _length::32, field_count::16, rest::binary>> ->
          fields = parse_field_values(rest, field_count, [])
          %__MODULE__{field_count: field_count, fields: fields}

        _ ->
          {:error, :invalid_data_row}
      end
    end

    defp parse_field_values(<<>>, 0, acc), do: Enum.reverse(acc)

    defp parse_field_values(data, count, acc) do
      case data do
        <<field_length::32, rest::binary>> ->
          case field_length do
            -1 ->
              # NULL field
              parse_field_values(rest, count - 1, [nil | acc])

            length when length >= 0 ->
              <<field_value::binary-size(length), rest2::binary>> = rest
              parse_field_values(rest2, count - 1, [field_value | acc])
          end

        _ ->
          {:error, :invalid_field_value}
      end
    end
  end

  defmodule CommandComplete do
    defstruct [:tag]

    def parse(data) do
      case data do
        <<"C", _length::32, rest::binary>> ->
          # Remove null terminator
          tag = String.slice(rest, 0, byte_size(rest) - 1)
          %__MODULE__{tag: tag}

        _ ->
          {:error, :invalid_command_complete}
      end
    end
  end

  # Message builders (existing code)
  def handshake_sequence do
    [
      authentication_ok(),
      parameter_status("server_version", "15.1"),
      parameter_status("server_encoding", "UTF8"),
      parameter_status("client_encoding", "UTF8"),
      parameter_status("application_name", "ex_db"),
      parameter_status("DateStyle", "ISO, MDY"),
      parameter_status("TimeZone", "UTC"),
      parameter_status("integer_datetimes", "on"),
      parameter_status("standard_conforming_strings", "on"),
      parameter_status("IntervalStyle", "postgres"),
      parameter_status("is_superuser", "off"),
      parameter_status("session_authorization", "testuser"),
      parameter_status("in_hot_standby", "off"),
      backend_key_data(),
      ready_for_query()
    ]
  end

  def authentication_ok do
    <<"R", 0, 0, 0, 8, 0, 0, 0, 0>>
  end

  def parameter_status(name, value) do
    data = "S" <> name <> <<0>> <> value <> <<0>>
    <<"S", byte_size(data) + 4::32, name::binary, 0, value::binary, 0>>
  end

  def backend_key_data do
    <<"K", 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2>>
  end

  def ready_for_query(state \\ ?I) do
    <<"Z", 0, 0, 0, 5, state>>
  end

  def error_response(severity, code, message) do
    error_data =
      "S" <>
        severity <>
        <<0>> <>
        "V" <>
        severity <>
        <<0>> <>
        "C" <>
        code <>
        <<0>> <>
        "M" <>
        message <>
        <<0>> <>
        <<0>>

    error_length = byte_size(error_data) + 4
    <<"E", error_length::32, error_data::binary>>
  end

  # New message builders for query responses
  def row_description(fields) do
    field_count = length(fields)
    field_data = Enum.map_join(fields, "", &build_field_description/1)
    data = <<field_count::16, field_data::binary>>
    <<"T", byte_size(data) + 4::32, data::binary>>
  end

  defp build_field_description(field) do
    name = Map.get(field, :name, "")
    table_oid = Map.get(field, :table_oid, 0)
    column_attr = Map.get(field, :column_attr, 0)
    # int4
    type_oid = Map.get(field, :type_oid, 23)
    type_size = Map.get(field, :type_size, 4)
    type_modifier = Map.get(field, :type_modifier, -1)
    # text format
    format_code = Map.get(field, :format_code, 0)

    name <>
      <<0>> <>
      <<table_oid::32, column_attr::16, type_oid::32, type_size::16, type_modifier::32,
        format_code::16>>
  end

  def data_row(values) do
    field_count = length(values)
    field_data = Enum.map_join(values, "", &build_field_value/1)
    data = <<field_count::16, field_data::binary>>
    <<"D", byte_size(data) + 4::32, data::binary>>
  end

  defp build_field_value(nil) do
    # NULL value
    <<-1::32>>
  end

  defp build_field_value(value) when is_binary(value) do
    <<byte_size(value)::32, value::binary>>
  end

  defp build_field_value(value) when is_integer(value) do
    value_str = Integer.to_string(value)
    <<byte_size(value_str)::32, value_str::binary>>
  end

  def command_complete(tag) do
    data = tag <> <<0>>
    <<"C", byte_size(data) + 4::32, data::binary>>
  end
end
