defmodule ExDb.QueryPlanner do
  @moduledoc """
  Query planner that analyzes SQL AST and generates optimized execution plans.

  The query planner is responsible for:
  - Analyzing WHERE clauses for index opportunities
  - Choosing between table scans and index lookups
  - Optimizing query execution strategies
  - Future: Cost-based optimization
  """

  alias ExDb.QueryPlan
  alias ExDb.SQL.AST.{SelectStatement, InsertStatement, UpdateStatement, CreateTableStatement}
  require Logger

  @doc """
  Creates an execution plan for the given SQL AST statement.

  ## Examples

      # Simple table scan
      iex> QueryPlanner.plan(%SelectStatement{from: %{name: "users"}, columns: [%{name: "*"}], where: nil})
      %QueryPlan.Node{type: :table_scan, table: "users", ...}

      # Potential index lookup (when indexes are implemented)
      iex> QueryPlanner.plan(%SelectStatement{from: %{name: "users"}, where: %{left: %{name: "id"}, operator: "=", right: %{value: 1}}})
      %QueryPlan.Node{type: :table_scan, table: "users", ...}  # For now, still table scan
  """
  @spec plan(
          SelectStatement.t()
          | InsertStatement.t()
          | UpdateStatement.t()
          | CreateTableStatement.t()
        ) ::
          QueryPlan.Node.t()
  def plan(%SelectStatement{} = statement), do: plan_select(statement)
  def plan(%InsertStatement{} = statement), do: plan_insert(statement)
  def plan(%UpdateStatement{} = statement), do: plan_update(statement)
  def plan(%CreateTableStatement{} = statement), do: plan_create_table(statement)

  # SELECT statement planning
  defp plan_select(%SelectStatement{from: nil, columns: columns}) do
    # SELECT without FROM (e.g., SELECT 1, 'hello')
    Logger.debug("Planning literal SELECT without FROM")

    QueryPlan.seq_scan(nil, columns,
      cost_start: 0.0,
      cost_total: 0.0,
      rows: 1,
      width: 8
    )
  end

  defp plan_select(%SelectStatement{from: table, columns: columns, where: where}) do
    table_name = table.name

    # Analyze WHERE clause for index opportunities
    case analyze_where_for_indexes(where) do
      {:index_candidate, index_type, index_column, condition} ->
        # Future: Check if index actually exists
        # For now, fall back to table scan
        Logger.debug("Found potential index opportunity",
          table: table_name,
          column: index_column,
          type: index_type
        )

        # TODO: When indexes are implemented, return index_lookup plan
        QueryPlan.seq_scan(table_name, columns,
          filter: where,
          rows: estimate_table_rows(table_name)
        )

      :no_index_opportunity ->
        Logger.debug("No index opportunity found, using table scan",
          table: table_name
        )

        QueryPlan.seq_scan(table_name, columns,
          filter: where,
          rows: estimate_table_rows(table_name)
        )
    end
  end

  # INSERT statement planning
  defp plan_insert(%InsertStatement{table: table, values: values}) do
    Logger.debug("Planning INSERT statement", table: table.name, value_count: length(values))

    QueryPlan.insert(table.name, values)
  end

  # UPDATE statement planning
  defp plan_update(%UpdateStatement{table: table, set: set, where: where}) do
    Logger.debug("Planning UPDATE statement",
      table: table.name,
      column: set.column.name
    )

    QueryPlan.update(table.name, set, where)
  end

  # CREATE TABLE statement planning
  defp plan_create_table(%CreateTableStatement{table: table, columns: columns}) do
    Logger.debug("Planning CREATE TABLE statement",
      table: table.name,
      column_count: if(columns, do: length(columns), else: 0)
    )

    # CREATE TABLE doesn't have a helper in QueryPlan yet, use seq_scan as placeholder
    QueryPlan.seq_scan(table.name, columns,
      cost_start: 0.0,
      cost_total: 1.0,
      rows: 0,
      width: 0
    )
    |> Map.put(:node_type, :create_table)
  end

  # Helper functions

  defp create_table_scan_plan(table_name, columns, where_condition) do
    estimated_cost = estimate_table_scan_cost(table_name)
    estimated_rows = estimate_table_rows(table_name)

    QueryPlan.seq_scan(table_name, columns,
      filter: where_condition,
      cost_total: estimated_cost,
      rows: estimated_rows,
      # default row width estimate
      width: 32
    )
  end

  defp analyze_where_for_indexes(nil), do: :no_index_opportunity

  defp analyze_where_for_indexes(%{left: left, operator: operator, right: right}) do
    # Look for equality conditions on columns (best for indexes)
    case {left, operator, right} do
      {%{name: column_name}, "=", %{type: _type, value: _value}} ->
        # Perfect for index lookup!
        {:index_candidate, :equality, column_name, {column_name, operator, right}}

      {%{name: column_name}, op, %{type: _type, value: _value}}
      when op in ["<", ">", "<=", ">="] ->
        # Good for range index scan
        {:index_candidate, :range, column_name, {column_name, operator, right}}

      _ ->
        # Complex conditions, functions, etc. - no index opportunity
        :no_index_opportunity
    end
  end

  defp estimate_table_scan_cost(_table_name) do
    # Future: Use table statistics to estimate cost
    # For now, return a simple default
    100.0
  end

  defp estimate_table_rows(_table_name) do
    # Future: Use table statistics to estimate row count
    # For now, return a simple default
    1000
  end

  @doc """
  Future: Check if a useful index exists for the given WHERE condition.
  """
  def has_useful_index?(_table_name, _where_condition) do
    # TODO: Implement when index management is added
    false
  end

  @doc """
  Future: Find the best index for a given WHERE condition.
  """
  def find_best_index(_table_name, _where_condition) do
    # TODO: Implement when index management is added
    :no_index
  end
end
