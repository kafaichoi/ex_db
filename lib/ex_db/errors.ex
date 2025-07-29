defmodule ExDb.Errors do
  @moduledoc """
  Domain-specific exceptions for ExDb.

  This module defines proper Elixir exceptions that represent business logic errors
  in our database system. These exceptions can be raised, rescued, and handled
  using standard Elixir exception mechanisms.

  For wire protocol representation, use `ExDb.Wire.ErrorMessage.from_exception/1`
  to convert these exceptions to PostgreSQL wire format.

  ## Exception Categories

  - **Table Errors**: `TableNotFoundError`, `TableAlreadyExistsError`
  - **SQL Errors**: `SyntaxError`, `UnsupportedFeatureError`
  - **Data Errors**: `TypeMismatchError`, `ColumnCountMismatchError`, `ValueTooLongError`
  - **Protocol Errors**: `ProtocolViolationError`, `ConnectionFailureError`
  """

  # Table-related errors
  defmodule TableNotFoundError do
    defexception [:table_name, :message]

    def exception(table_name) when is_binary(table_name) do
      %__MODULE__{
        table_name: table_name,
        message: "relation \"#{table_name}\" does not exist"
      }
    end
  end

  defmodule TableAlreadyExistsError do
    defexception [:table_name, :message]

    def exception(table_name) when is_binary(table_name) do
      %__MODULE__{
        table_name: table_name,
        message: "relation \"#{table_name}\" already exists"
      }
    end
  end

  # SQL parsing and syntax errors
  defmodule SyntaxError do
    defexception [:query, :details, :message]

    def exception(opts) when is_list(opts) do
      query = Keyword.get(opts, :query, "")
      details = Keyword.get(opts, :details)

      message =
        if details do
          "syntax error: #{details}"
        else
          "syntax error in query"
        end

      %__MODULE__{
        # Limit query length
        query: String.slice(query, 0, 100),
        details: details,
        message: message
      }
    end
  end

  defmodule UnsupportedFeatureError do
    defexception [:feature, :message]

    def exception(feature) when is_binary(feature) do
      %__MODULE__{
        feature: feature,
        message: "feature not supported: #{feature}"
      }
    end
  end

  # Data validation errors
  defmodule TypeMismatchError do
    defexception [:column_name, :provided_type, :expected_type, :message]

    def exception(opts) when is_list(opts) do
      column = Keyword.fetch!(opts, :column_name)
      provided = Keyword.fetch!(opts, :provided_type)
      expected = Keyword.fetch!(opts, :expected_type)

      %__MODULE__{
        column_name: column,
        provided_type: provided,
        expected_type: expected,
        message:
          "column \"#{column}\" is of type #{expected} but expression is of type #{provided}"
      }
    end
  end

  defmodule ColumnCountMismatchError do
    defexception [:provided_count, :expected_count, :table_name, :message]

    def exception(opts) when is_list(opts) do
      provided = Keyword.fetch!(opts, :provided)
      expected = Keyword.fetch!(opts, :expected)
      table_name = Keyword.get(opts, :table_name)

      message =
        if table_name do
          "INSERT has more expressions than target columns for table \"#{table_name}\""
        else
          "INSERT has more expressions than target columns"
        end

      %__MODULE__{
        provided_count: provided,
        expected_count: expected,
        table_name: table_name,
        message: message
      }
    end
  end

  defmodule ValueTooLongError do
    defexception [:column_name, :length, :max_length, :message]

    def exception(opts) when is_list(opts) do
      column = Keyword.fetch!(opts, :column_name)
      length = Keyword.fetch!(opts, :length)
      max_length = Keyword.fetch!(opts, :max_length)

      %__MODULE__{
        column_name: column,
        length: length,
        max_length: max_length,
        message: "value too long for type character varying(#{max_length})"
      }
    end
  end

  # Protocol and connection errors
  defmodule ProtocolViolationError do
    defexception [:details, :message]

    def exception(details) when is_binary(details) do
      %__MODULE__{
        details: details,
        message: "protocol violation"
      }
    end
  end

  defmodule ConnectionFailureError do
    defexception [:reason, :message]

    def exception(reason) when is_binary(reason) do
      %__MODULE__{
        reason: reason,
        message: "connection failure"
      }
    end
  end

  # Internal server errors
  defmodule InternalError do
    defexception [:reason, :message]

    def exception(reason) do
      %__MODULE__{
        reason: reason,
        message: "internal error"
      }
    end
  end

  @doc """
  Converts executor error tuples to proper exceptions.

  ## Examples

      iex> ExDb.Errors.from_executor_error({:table_not_found, "users"})
      %ExDb.Errors.TableNotFoundError{table_name: "users", ...}
  """
  def from_executor_error({:table_not_found, table_name}),
    do: TableNotFoundError.exception(table_name)

  def from_executor_error({:table_already_exists, table_name}),
    do: TableAlreadyExistsError.exception(table_name)

  def from_executor_error({:column_count_mismatch, provided, expected}),
    do: ColumnCountMismatchError.exception(provided: provided, expected: expected)

  def from_executor_error({:type_mismatch, column, provided, expected}),
    do:
      TypeMismatchError.exception(
        column_name: column,
        provided_type: provided,
        expected_type: expected
      )

  def from_executor_error({:value_too_long, column, length, max}),
    do: ValueTooLongError.exception(column_name: column, length: length, max_length: max)

  def from_executor_error(reason), do: InternalError.exception(reason)

  @doc """
  Converts parser error strings to proper exceptions.
  """
  def from_parser_error(reason, query \\ "") do
    case reason do
      "Unexpected token: " <> _details ->
        UnsupportedFeatureError.exception("query type")

      details when details in ["Empty query", "Empty token list"] ->
        SyntaxError.exception(query: query, details: "empty query string")

      details when is_binary(details) ->
        SyntaxError.exception(query: query, details: details)

      _other ->
        SyntaxError.exception(query: query)
    end
  end
end
