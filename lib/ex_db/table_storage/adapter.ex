defmodule ExDb.TableStorage.Adapter do
  @moduledoc """
  Behavior for storage adapters that can store and retrieve table data.

  This behavior defines the interface that all storage implementations must follow,
  allowing the query executor to work with different storage backends (in-memory,
  disk-based, etc.) without knowing the implementation details.
  """

  @doc """
  Creates a new table with the given name and optional column definitions.

  Returns the updated adapter state on success.
  """
  @callback create_table(
              adapter_state :: term(),
              table_name :: String.t(),
              columns :: [ExDb.SQL.AST.ColumnDefinition.t()] | nil
            ) ::
              {:ok, term()} | {:error, term()}

  @doc """
  Checks if a table with the given name exists.
  """
  @callback table_exists?(adapter_state :: term(), table_name :: String.t()) ::
              boolean()

  @doc """
  Gets the schema (column definitions) for a table.

  Returns the list of column definitions and the adapter state.
  """
  @callback get_table_schema(adapter_state :: term(), table_name :: String.t()) ::
              {:ok, [ExDb.SQL.AST.ColumnDefinition.t()], term()} | {:error, term()}

  @doc """
  Inserts a row of values into the specified table.

  Values should be a list in the same order as they appear in the INSERT statement.
  Returns the updated adapter state on success.
  """
  @callback insert_row(adapter_state :: term(), table_name :: String.t(), values :: [term()]) ::
              {:ok, term()} | {:error, term()}

  @doc """
  Updates rows in the specified table that match the given condition.

  For now, this is a simple implementation that updates all matching rows.
  Returns {updated_count, new_adapter_state} on success.
  """
  @callback update_row(
              adapter_state :: term(),
              table_name :: String.t(),
              column_name :: String.t(),
              new_value :: term(),
              where_condition :: ExDb.SQL.AST.BinaryOp.t() | nil
            ) ::
              {:ok, non_neg_integer(), term()} | {:error, term()}

  @doc """
  Retrieves all rows from the specified table.

  Returns a list of rows (where each row is a list of values) and the adapter state.
  """
  @callback select_all_rows(adapter_state :: term(), table_name :: String.t()) ::
              {:ok, [list()], term()} | {:error, term()}

  @doc """
  Gets basic information about a table structure.

  Returns metadata about the table. For now, this is minimal but will be expanded
  as we add schema support.
  """
  @callback table_info(adapter_state :: term(), table_name :: String.t()) ::
              {:ok, map(), term()} | {:error, term()}
end
