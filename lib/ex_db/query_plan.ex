defmodule ExDb.QueryPlan do
  @moduledoc """
  PostgreSQL-style query execution plan representation.

  Query plans are tree structures where each node represents an operation.
  Each node can have children (for joins, subqueries, etc.) and contains
  cost estimates, row estimates, and operation-specific properties.

  ## Node Types
  - `:seq_scan` - Sequential scan of a table
  - `:index_scan` - Index-based scan (future)
  - `:filter` - Filter operation (future)
  - `:hash_join` - Hash join (future)
  - `:nested_loop` - Nested loop join (future)
  - `:insert` - Insert operation
  - `:update` - Update operation
  - `:delete` - Delete operation (future)
  """

  defmodule Node do
    @moduledoc """
    A single node in the query execution plan tree.

    Each node represents one operation (scan, join, filter, etc.)
    and can have child nodes for complex operations.
    """

    defstruct [
      # :seq_scan, :index_scan, :hash_join, etc.
      :node_type,
      # table name (for scan nodes)
      :relation,
      # index name (for index scan nodes)
      :index,
      # filter condition (WHERE clause)
      :filter,
      # join condition (for join nodes)
      :join_cond,
      # columns to return (SELECT list)
      :target_list,
      # list of child nodes (for joins, subqueries)
      :children,
      # startup cost estimate
      :cost_start,
      # total cost estimate
      :cost_total,
      # estimated number of rows
      :rows,
      # estimated average row width in bytes
      :width,
      # additional node-specific properties
      :properties
    ]

    # Sequential table scan
    @type node_type ::
            :seq_scan
            # Index scan (future)
            | :index_scan
            # Filter operation (future)
            | :filter
            # Hash join (future)
            | :hash_join
            # Nested loop join (future)
            | :nested_loop
            # Insert operation
            | :insert
            # Update operation
            | :update
            # Delete operation (future)
            | :delete

    @type t :: %__MODULE__{
            node_type: node_type(),
            relation: String.t() | nil,
            index: String.t() | nil,
            filter: ExDb.SQL.AST.BinaryOp.t() | nil,
            join_cond: ExDb.SQL.AST.BinaryOp.t() | nil,
            target_list: [ExDb.SQL.AST.Column.t()] | nil,
            children: [t()] | nil,
            cost_start: float() | nil,
            cost_total: float() | nil,
            rows: non_neg_integer() | nil,
            width: non_neg_integer() | nil,
            properties: map() | nil
          }
  end

  @doc """
  Creates a sequential scan plan node.

  ## Examples

      # Simple table scan
      QueryPlan.seq_scan("users", ["*"])

      # Table scan with filter
      QueryPlan.seq_scan("users", ["*"], filter: where_condition)
  """
  def seq_scan(table_name, target_list, opts \\ []) do
    %Node{
      node_type: :seq_scan,
      relation: table_name,
      target_list: target_list,
      filter: Keyword.get(opts, :filter),
      cost_start: Keyword.get(opts, :cost_start, 0.0),
      cost_total: Keyword.get(opts, :cost_total, 100.0),
      rows: Keyword.get(opts, :rows, 1000),
      width: Keyword.get(opts, :width, 32),
      children: nil,
      index: nil,
      join_cond: nil,
      properties: Keyword.get(opts, :properties, %{})
    }
  end

  @doc """
  Creates an index scan plan node (future).
  """
  def index_scan(table_name, index_name, target_list, opts \\ []) do
    %Node{
      node_type: :index_scan,
      relation: table_name,
      index: index_name,
      target_list: target_list,
      filter: Keyword.get(opts, :filter),
      cost_start: Keyword.get(opts, :cost_start, 0.1),
      cost_total: Keyword.get(opts, :cost_total, 10.0),
      rows: Keyword.get(opts, :rows, 1),
      width: Keyword.get(opts, :width, 32),
      children: nil,
      join_cond: nil,
      properties: Keyword.get(opts, :properties, %{})
    }
  end

  @doc """
  Creates an INSERT plan node.
  """
  def insert(table_name, values, opts \\ []) do
    %Node{
      node_type: :insert,
      relation: table_name,
      target_list: values,
      cost_start: Keyword.get(opts, :cost_start, 0.0),
      cost_total: Keyword.get(opts, :cost_total, 1.0),
      rows: Keyword.get(opts, :rows, 1),
      width: Keyword.get(opts, :width, 32),
      children: nil,
      filter: nil,
      index: nil,
      join_cond: nil,
      properties: Keyword.get(opts, :properties, %{})
    }
  end

  @doc """
  Creates an UPDATE plan node.
  """
  def update(table_name, set_clause, where_condition, opts \\ []) do
    %Node{
      node_type: :update,
      relation: table_name,
      target_list: [set_clause],
      filter: where_condition,
      cost_start: Keyword.get(opts, :cost_start, 0.0),
      cost_total: Keyword.get(opts, :cost_total, 50.0),
      rows: Keyword.get(opts, :rows, 10),
      width: Keyword.get(opts, :width, 32),
      children: nil,
      index: nil,
      join_cond: nil,
      properties: Keyword.get(opts, :properties, %{})
    }
  end

  @doc """
  Pretty prints a query plan in PostgreSQL EXPLAIN format.

  ## Examples

      iex> plan = QueryPlan.seq_scan("users", ["*"], filter: where_condition, rows: 3)
      iex> QueryPlan.explain(plan)
      "Seq Scan on users  (cost=0.00..100.00 rows=3 width=32)\\n  Filter: (id = 1)"
  """
  def explain(%Node{} = node, indent \\ 0) do
    padding = String.duplicate("  ", indent)

    # Main node description
    main_line =
      case node.node_type do
        :seq_scan ->
          cost_info = format_cost(node.cost_start, node.cost_total, node.rows, node.width)
          "#{padding}Seq Scan on #{node.relation}  #{cost_info}"

        :index_scan ->
          cost_info = format_cost(node.cost_start, node.cost_total, node.rows, node.width)
          "#{padding}Index Scan using #{node.index} on #{node.relation}  #{cost_info}"

        :insert ->
          cost_info = format_cost(node.cost_start, node.cost_total, node.rows, node.width)
          "#{padding}Insert on #{node.relation}  #{cost_info}"

        :update ->
          cost_info = format_cost(node.cost_start, node.cost_total, node.rows, node.width)
          "#{padding}Update on #{node.relation}  #{cost_info}"

        other ->
          "#{padding}#{other}"
      end

    # Add filter information
    filter_line =
      case node.filter do
        nil -> ""
        filter -> "\n#{padding}  Filter: #{format_condition(filter)}"
      end

    # Add child nodes
    children_lines =
      case node.children do
        nil ->
          ""

        [] ->
          ""

        children ->
          children
          |> Enum.map(&explain(&1, indent + 1))
          |> Enum.join("\n")
          |> then(&("\n" <> &1))
      end

    main_line <> filter_line <> children_lines
  end

  # Helper functions

  defp format_cost(start_cost, total_cost, rows, width) do
    "(cost=#{:erlang.float_to_binary(start_cost, decimals: 2)}..#{:erlang.float_to_binary(total_cost, decimals: 2)} rows=#{rows} width=#{width})"
  end

  defp format_condition(%{left: left, operator: op, right: right}) do
    left_str =
      case left do
        %{name: name} -> name
        %{value: value} -> inspect(value)
      end

    right_str =
      case right do
        %{name: name} -> name
        %{value: value} when is_binary(value) -> "'#{value}'"
        %{value: value} -> inspect(value)
      end

    "(#{left_str} #{op} #{right_str})"
  end

  defp format_condition(other), do: inspect(other)
end
