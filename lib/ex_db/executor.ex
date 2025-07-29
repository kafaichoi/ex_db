defmodule ExDb.Executor do
  @moduledoc """
  Executes SQL statements against a storage adapter.

  The executor takes parsed AST statements and executes them against
  the provided storage adapter, returning results in a consistent format.
  """

  alias ExDb.SQL.AST.{InsertStatement, SelectStatement, CreateTableStatement, UpdateStatement}

  # SQL constants
  @anonymous_column_name "?column?"

  require Logger

  @doc """
  Executes a SQL statement against the given storage adapter.

  ## Parameters
  - `ast`: The parsed AST statement to execute
  - `adapter`: A tuple of {adapter_module, adapter_state}

  ## Returns
  - For INSERT: `{:ok, adapter}` where adapter contains updated state
  - For SELECT: `{:ok, result, columns, adapter}` where result is a list of rows and columns is metadata
  - For CREATE TABLE: `{:ok, adapter}` where adapter contains updated state
  - For errors: `{:error, reason}`
  """
  @spec execute(
          InsertStatement.t() | SelectStatement.t() | CreateTableStatement.t(),
          {module(), any()}
        ) ::
          {:ok, {module(), any()}}
          | {:ok, list(list()), list(map()), {module(), any()}}
          | {:error, any()}
  def execute(ast, adapter)

  def execute(%InsertStatement{} = insert_stmt, adapter) do
    execute_insert(insert_stmt, adapter)
  end

  def execute(%SelectStatement{} = select_stmt, adapter) do
    execute_select(select_stmt, adapter)
  end

  def execute(%UpdateStatement{} = update_stmt, adapter) do
    execute_update(update_stmt, adapter)
  end

  def execute(%CreateTableStatement{} = create_stmt, adapter) do
    execute_create_table(create_stmt, adapter)
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

        # Try to get table schema for validation (if it exists)
        case adapter_module.get_table_schema(adapter_state, table_name) do
          {:ok, schema, adapter_state} ->
            # Validate values against schema
            case validate_insert_values(row_values, schema) do
              :ok ->
                case adapter_module.insert_row(adapter_state, table_name, row_values) do
                  {:ok, new_adapter_state} ->
                    {:ok, {adapter_module, new_adapter_state}}

                  {:error, reason} ->
                    {:error, reason}
                end

              {:error, reason} ->
                {:error, reason}
            end

          {:error, {:table_not_found, _}} ->
            # Legacy table without schema, skip validation
            case adapter_module.insert_row(adapter_state, table_name, row_values) do
              {:ok, new_adapter_state} ->
                {:ok, {adapter_module, new_adapter_state}}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      false ->
        {:error, {:table_not_found, table_name}}
    end
  end

  defp execute_update(
         %UpdateStatement{table: table, set: set, where: where},
         {adapter_module, adapter_state}
       ) do
    table_name = table.name

    case adapter_module.table_exists?(adapter_state, table_name) do
      true ->
        # Extract column and value from set clause
        # Currently only supports single column updates
        column_name = set.column.name
        new_value = set.value.value

        case adapter_module.update_row(adapter_state, table_name, column_name, new_value, where) do
          {:ok, updated_count, new_adapter_state} ->
            Logger.debug("UPDATE executed successfully",
              table: table_name,
              column: column_name,
              updated_count: updated_count
            )

            {:ok, {adapter_module, new_adapter_state}}

          {:error, reason} ->
            {:error, reason}
        end

      false ->
        {:error, {:table_not_found, table_name}}
    end
  end

  defp execute_select(
         %SelectStatement{from: table, columns: columns, where: where},
         {adapter_module, adapter_state}
       ) do
    case table do
      nil ->
        # SELECT without FROM clause (e.g., "SELECT 1")
        # Evaluate the literals directly
        row = evaluate_literals(columns)
        column_info = build_literal_column_info(columns)
        {:ok, [row], column_info, {adapter_module, adapter_state}}

      %{name: table_name} ->
        # SELECT with FROM clause
        case adapter_module.table_exists?(adapter_state, table_name) do
          true ->
            case adapter_module.select_all_rows(adapter_state, table_name) do
              {:ok, rows, new_adapter_state} ->
                # Apply WHERE clause filtering
                filtered_rows = apply_where_filter(rows, where)

                # Get column information based on the query
                column_info =
                  build_select_column_info(columns, table_name, adapter_module, adapter_state)

                {:ok, filtered_rows, column_info, {adapter_module, new_adapter_state}}

              {:error, reason} ->
                {:error, reason}
            end

          false ->
            {:error, {:table_not_found, table_name}}
        end
    end
  end

  defp execute_create_table(
         %CreateTableStatement{table: table, columns: columns},
         {adapter_module, adapter_state}
       ) do
    table_name = table.name

    case adapter_module.table_exists?(adapter_state, table_name) do
      true ->
        {:error, {:table_already_exists, table_name}}

      false ->
        case adapter_module.create_table(adapter_state, table_name, columns) do
          {:ok, new_adapter_state} ->
            {:ok, {adapter_module, new_adapter_state}}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  # Validate INSERT values against table schema
  defp validate_insert_values(values, schema) do
    # If no schema (legacy table) or empty schema, skip validation
    if schema == nil or Enum.empty?(schema) do
      :ok
    else
      # Check column count
      if length(values) != length(schema) do
        {:error, {:column_count_mismatch, length(values), length(schema)}}
      else
        # Validate each value against its column type
        validate_column_types(values, schema)
      end
    end
  end

  defp validate_column_types(values, schema) do
    values
    |> Enum.zip(schema)
    |> Enum.reduce_while(:ok, fn {value, column_def}, acc ->
      case validate_value_type(value, column_def) do
        :ok -> {:cont, acc}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp validate_value_type(value, column_def) do
    case {value, column_def.type} do
      {val, :integer} when is_integer(val) ->
        :ok

      {val, :text} when is_binary(val) ->
        :ok

      {val, :varchar} when is_binary(val) ->
        # Check length constraint if specified
        if column_def.size && String.length(val) > column_def.size do
          {:error, {:value_too_long, column_def.name, String.length(val), column_def.size}}
        else
          :ok
        end

      {val, :boolean} when is_boolean(val) ->
        :ok

      {val, expected_type} ->
        actual_type =
          cond do
            is_integer(val) -> :integer
            is_binary(val) -> :text
            is_boolean(val) -> :boolean
            is_float(val) -> :float
            true -> :unknown
          end

        {:error, {:type_mismatch, column_def.name, actual_type, expected_type}}
    end
  end

  # Helper to evaluate literal values in SELECT without FROM
  defp evaluate_literals(columns) do
    Enum.map(columns, fn
      %{type: :number, value: value} -> value
      %{type: :string, value: value} -> value
      %{name: "*"} -> "*"
      %{name: name} -> name
      other -> inspect(other)
    end)
  end

  # WHERE clause evaluation functions
  defp apply_where_filter(rows, nil), do: rows

  defp apply_where_filter(rows, where_condition) do
    Enum.filter(rows, fn row ->
      evaluate_where_condition(row, where_condition)
    end)
  end

  defp evaluate_where_condition(_row, nil), do: true

  defp evaluate_where_condition(row, %{left: left, operator: operator, right: right}) do
    left_value = evaluate_expression(row, left)
    right_value = evaluate_expression(row, right)

    # Handle both string and atom operators (same as in storage layer)
    case operator do
      "=" -> left_value == right_value
      "!=" -> left_value != right_value
      "<" -> left_value < right_value
      ">" -> left_value > right_value
      "<=" -> left_value <= right_value
      ">=" -> left_value >= right_value
      :eq -> left_value == right_value
      :ne -> left_value != right_value
      :lt -> left_value < right_value
      :le -> left_value <= right_value
      :gt -> left_value > right_value
      :ge -> left_value >= right_value
      _ -> false
    end
  end

  defp evaluate_expression(_row, %{type: _type, value: value}), do: value

  defp evaluate_expression(row, %{name: column_name}) do
    # For simple implementation, assume columns are in order: id, name, email, etc.
    # This is a simplified approach - in production you'd use column metadata
    case column_name do
      "id" -> Enum.at(row, 0)
      "name" -> Enum.at(row, 1)
      "email" -> Enum.at(row, 2)
      _ -> nil
    end
  end

  # Build column info for SELECT without FROM clause (literals)
  defp build_literal_column_info(columns) do
    Enum.map(columns, fn
      %{type: :number} -> %{name: @anonymous_column_name, type: :integer}
      %{type: :string} -> %{name: @anonymous_column_name, type: :text}
      %{type: :boolean} -> %{name: @anonymous_column_name, type: :boolean}
      %{name: name} -> %{name: name, type: :text}
      _ -> %{name: @anonymous_column_name, type: :text}
    end)
  end

  # Build column info for SELECT with FROM clause
  defp build_select_column_info(columns, table_name, adapter_module, adapter_state) do
    case adapter_module.get_table_schema(adapter_state, table_name) do
      {:ok, schema, _adapter_state} when is_list(schema) ->
        # Table has schema, use it to build column info
        build_column_info_from_schema(columns, schema)

      {:error, {:table_not_found, _}} ->
        # Legacy table without schema, fall back to generic column info
        build_generic_column_info(columns)

      _ ->
        # No schema available, use generic column info
        build_generic_column_info(columns)
    end
  end

  # Build column info using table schema
  defp build_column_info_from_schema(columns, schema) do
    Enum.map(columns, fn
      %{name: "*"} ->
        # SELECT * - return all columns from schema
        Enum.map(schema, fn col_def ->
          %{name: col_def.name, type: col_def.type}
        end)

      %{name: column_name} ->
        # SELECT specific_column - find it in schema
        case Enum.find(schema, fn col_def -> col_def.name == column_name end) do
          # Column not found in schema
          nil -> %{name: column_name, type: :text}
          col_def -> %{name: col_def.name, type: col_def.type}
        end

      _ ->
        # Literal in SELECT list
        %{name: @anonymous_column_name, type: :text}
    end)
    |> List.flatten()
  end

  # Build generic column info when no schema is available
  defp build_generic_column_info(columns) do
    Enum.map(columns, fn
      %{name: "*"} -> %{name: @anonymous_column_name, type: :text}
      %{name: column_name} -> %{name: column_name, type: :text}
      _ -> %{name: @anonymous_column_name, type: :text}
    end)
  end
end
