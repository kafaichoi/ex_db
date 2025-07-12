defmodule ExDb.Executor do
  @moduledoc """
  Executes SQL statements against a storage adapter.

  The executor takes parsed AST statements and executes them against
  the provided storage adapter, returning results in a consistent format.
  """

  alias ExDb.SQL.AST.{InsertStatement, SelectStatement}

  @doc """
  Executes a SQL statement against the given storage adapter.

  ## Parameters
  - `ast`: The parsed AST statement to execute
  - `adapter`: A tuple of {adapter_module, adapter_state}

  ## Returns
  - For INSERT: `{:ok, adapter}` where adapter contains updated state
  - For SELECT: `{:ok, result, adapter}` where result is a list of rows
  - For errors: `{:error, reason}`
  """
  @spec execute(InsertStatement.t() | SelectStatement.t(), {module(), any()}) ::
          {:ok, {module(), any()}} | {:ok, list(list()), {module(), any()}} | {:error, any()}
  def execute(ast, adapter)

  def execute(%InsertStatement{} = insert_stmt, adapter) do
    execute_insert(insert_stmt, adapter)
  end

  def execute(%SelectStatement{} = select_stmt, adapter) do
    execute_select(select_stmt, adapter)
  end

  # Private functions for handling specific statement types
  defp execute_insert(
         %InsertStatement{table: table, values: values},
         {adapter_module, adapter_state}
       ) do
    table_name = table.name

    case adapter_module.table_exists?(adapter_state, table_name) do
      true ->
        # Convert literal values to their actual values
        row_values = Enum.map(values, fn %{value: value} -> value end)

        case adapter_module.insert_row(adapter_state, table_name, row_values) do
          {:ok, new_adapter_state} ->
            {:ok, {adapter_module, new_adapter_state}}

          {:error, reason} ->
            {:error, reason}
        end

      false ->
        {:error, {:table_not_found, table_name}}
    end
  end

  defp execute_select(%SelectStatement{from: table}, {adapter_module, adapter_state}) do
    table_name = table.name

    case adapter_module.table_exists?(adapter_state, table_name) do
      true ->
        case adapter_module.select_all_rows(adapter_state, table_name) do
          {:ok, rows, new_adapter_state} ->
            {:ok, rows, {adapter_module, new_adapter_state}}

          {:error, reason} ->
            {:error, reason}
        end

      false ->
        {:error, {:table_not_found, table_name}}
    end
  end
end
