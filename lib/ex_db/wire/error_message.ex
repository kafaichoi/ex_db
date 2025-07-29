defmodule ExDb.Wire.ErrorMessage do
  @moduledoc """
  Parser for incoming PostgreSQL wire protocol error messages.

  This module handles parsing error messages we RECEIVE (e.g., from PostgreSQL servers
  or for testing our own error message format). For CREATING and SENDING errors,
  use `ExDb.Errors` instead.

  ## Purpose

  - **Testing**: Parse error responses from our own server to verify correctness
  - **Client functionality**: Parse errors from real PostgreSQL servers (future use)
  - **Wire protocol completeness**: Full PostgreSQL ErrorResponse support

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

  # PostgreSQL SQLSTATE codes
  @syntax_error "42601"
  @undefined_table "42P01"
  @duplicate_table "42P07"
  @invalid_text_representation "22P02"
  @string_data_length_mismatch "22026"
  @feature_not_supported "0A000"
  @protocol_violation "08P01"
  @connection_failure "08006"
  @internal_error "XX000"

  @doc """
  Convert an ExDb.Errors exception to wire protocol format.

  ## Examples

      iex> exception = ExDb.Errors.TableNotFoundError.exception("users")
      iex> ExDb.Wire.ErrorMessage.from_exception(exception)
      %ExDb.Wire.ErrorMessage{
        severity: "ERROR",
        code: "42P01",
        message: "relation \"users\" does not exist",
        table_name: "users",
        hint: "Check the table name and ensure it has been created."
      }
  """
  def from_exception(%ExDb.Errors.TableNotFoundError{table_name: table_name, message: message}) do
    %__MODULE__{
      severity: "ERROR",
      severity_v: "ERROR",
      code: @undefined_table,
      message: message,
      detail: nil,
      hint: "Check the table name and ensure it has been created.",
      table_name: table_name
    }
  end

  def from_exception(%ExDb.Errors.TableAlreadyExistsError{
        table_name: table_name,
        message: message
      }) do
    %__MODULE__{
      severity: "ERROR",
      severity_v: "ERROR",
      code: @duplicate_table,
      message: message,
      detail: nil,
      hint: "Use a different table name or DROP the existing table first.",
      table_name: table_name
    }
  end

  def from_exception(%ExDb.Errors.SyntaxError{query: query, details: details, message: message}) do
    %__MODULE__{
      severity: "ERROR",
      severity_v: "ERROR",
      code: @syntax_error,
      message: message,
      detail: details,
      hint: "Check the query syntax and try again.",
      internal_query: query
    }
  end

  def from_exception(%ExDb.Errors.UnsupportedFeatureError{feature: _feature, message: message}) do
    %__MODULE__{
      severity: "ERROR",
      severity_v: "ERROR",
      code: @feature_not_supported,
      message: message,
      detail: nil,
      hint: "This feature is not yet implemented in ExDb."
    }
  end

  def from_exception(%ExDb.Errors.TypeMismatchError{column_name: column, message: message}) do
    %__MODULE__{
      severity: "ERROR",
      severity_v: "ERROR",
      code: @invalid_text_representation,
      message: message,
      detail: nil,
      hint: "You will need to rewrite or cast the expression.",
      column_name: column
    }
  end

  def from_exception(%ExDb.Errors.ColumnCountMismatchError{
        provided_count: provided,
        expected_count: expected,
        table_name: table_name,
        message: message
      }) do
    %__MODULE__{
      severity: "ERROR",
      severity_v: "ERROR",
      code: @string_data_length_mismatch,
      message: message,
      detail: "Expected #{expected} columns but got #{provided}",
      hint: "Check the column count in your INSERT statement.",
      table_name: table_name
    }
  end

  def from_exception(%ExDb.Errors.ValueTooLongError{
        column_name: column,
        length: length,
        max_length: max,
        message: message
      }) do
    %__MODULE__{
      severity: "ERROR",
      severity_v: "ERROR",
      code: @string_data_length_mismatch,
      message: message,
      detail: "Value length #{length} exceeds maximum #{max} for column \"#{column}\"",
      hint: "Reduce the length of the input value.",
      column_name: column
    }
  end

  def from_exception(%ExDb.Errors.ProtocolViolationError{details: details, message: message}) do
    %__MODULE__{
      # Protocol violations are fatal
      severity: "FATAL",
      severity_v: "FATAL",
      code: @protocol_violation,
      message: message,
      detail: details,
      hint: "Check client connection and protocol version."
    }
  end

  def from_exception(%ExDb.Errors.ConnectionFailureError{reason: reason, message: message}) do
    %__MODULE__{
      severity: "FATAL",
      severity_v: "FATAL",
      code: @connection_failure,
      message: message,
      detail: reason,
      hint: "Check network connectivity and server status."
    }
  end

  def from_exception(%ExDb.Errors.InternalError{reason: reason, message: message}) do
    %__MODULE__{
      severity: "ERROR",
      severity_v: "ERROR",
      code: @internal_error,
      message: message,
      detail: inspect(reason),
      hint: "This appears to be a server issue. Please report this bug."
    }
  end

  # Fallback for unknown exception types
  def from_exception(exception) do
    %__MODULE__{
      severity: "ERROR",
      severity_v: "ERROR",
      code: @internal_error,
      message: "internal error",
      detail: inspect(exception),
      hint: "Unknown exception type encountered."
    }
  end

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

  defp parse_fields(<<field_type, 0, rest::binary>>, acc) do
    # Find the null terminator for the value
    case :binary.split(rest, <<0>>) do
      [value, remaining] ->
        field_name = get_field_name(field_type)
        parse_fields(remaining, Map.put(acc, field_name, value))

      [value] ->
        # Last field, no more data
        field_name = get_field_name(field_type)
        Map.put(acc, field_name, value)
    end
  end

  # Handle case where there's no null byte after field type (malformed)
  defp parse_fields(_data, acc), do: acc

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
