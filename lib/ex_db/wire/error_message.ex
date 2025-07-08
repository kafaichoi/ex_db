defmodule ExDb.Wire.ErrorMessage do
  @moduledoc """
  ErrorMessage struct and parser for Postgres wire protocol error messages.
  Based on PostgreSQL 17 error fields specification.
  """

  @doc """
  Error message struct with all possible Postgres error fields.
  """
  defstruct [
    # Always present fields
    # S - Severity (ERROR, FATAL, PANIC, etc.)
    :severity,
    # V - Severity (non-localized version)
    :severity_v,
    # C - SQLSTATE code
    :code,
    # M - Primary human-readable error message
    :message,

    # Optional fields
    # D - Secondary error message with more detail
    :detail,
    # H - Suggestion what to do about the problem
    :hint,
    # P - Error cursor position (decimal ASCII integer)
    :position,
    # p - Internal cursor position
    :internal_position,
    # q - Text of failed internally-generated command
    :internal_query,
    # W - Context where error occurred (call stack traceback)
    :where,
    # s - Schema name if error associated with specific object
    :schema_name,
    # t - Table name if error associated with specific table
    :table_name,
    # c - Column name if error associated with specific column
    :column_name,
    # d - Data type name if error associated with specific type
    :data_type_name,
    # n - Constraint name if error associated with specific constraint
    :constraint_name,
    # F - File name of source-code location
    :file,
    # L - Line number of source-code location
    :line,
    # R - Name of source-code routine reporting error
    :routine
  ]

  @doc """
  Parse a Postgres ErrorResponse message binary into an ErrorMessage struct.
  """
  def parse(<<"E", _length::32, rest::binary>>) do
    fields = parse_fields(rest, %{})
    struct(__MODULE__, fields)
  end

  def parse(_), do: {:error, :invalid_error_message}

  defp parse_fields(<<>>, acc), do: acc
  defp parse_fields(<<0>>, acc), do: acc

  defp parse_fields(<<field_type, rest::binary>>, acc) do
    case String.split(rest, <<0>>, parts: 2) do
      [value, remaining] ->
        field_name = get_field_name(field_type)
        parse_fields(remaining, Map.put(acc, field_name, value))

      _ ->
        # No null terminator found, treat as end of message
        acc
    end
  end

  defp get_field_name(?S), do: :severity
  defp get_field_name(?V), do: :severity_v
  defp get_field_name(?C), do: :code
  defp get_field_name(?M), do: :message
  defp get_field_name(?D), do: :detail
  defp get_field_name(?H), do: :hint
  defp get_field_name(?P), do: :position
  defp get_field_name(?p), do: :internal_position
  defp get_field_name(?q), do: :internal_query
  defp get_field_name(?W), do: :where
  defp get_field_name(?s), do: :schema_name
  defp get_field_name(?t), do: :table_name
  defp get_field_name(?c), do: :column_name
  defp get_field_name(?d), do: :data_type_name
  defp get_field_name(?n), do: :constraint_name
  defp get_field_name(?F), do: :file
  defp get_field_name(?L), do: :line
  defp get_field_name(?R), do: :routine
  defp get_field_name(_), do: :unknown

  @doc """
  Convert ErrorMessage struct to a readable string for debugging.
  """
  def to_string(%__MODULE__{} = error) do
    parts = []
    parts = if error.severity, do: ["Severity: #{error.severity}" | parts], else: parts
    parts = if error.code, do: ["Code: #{error.code}" | parts], else: parts
    parts = if error.message, do: ["Message: #{error.message}" | parts], else: parts
    parts = if error.detail, do: ["Detail: #{error.detail}" | parts], else: parts
    parts = if error.hint, do: ["Hint: #{error.hint}" | parts], else: parts

    Enum.join(Enum.reverse(parts), "\n")
  end

  @doc """
  Check if this is a fatal error.
  """
  def fatal?(%__MODULE__{severity: severity}) when severity in ["FATAL", "PANIC"], do: true
  def fatal?(_), do: false

  @doc """
  Check if this is an error (not just a notice/warning).
  """
  def error?(%__MODULE__{severity: severity}) when severity in ["ERROR", "FATAL", "PANIC"],
    do: true

  def error?(_), do: false
end
