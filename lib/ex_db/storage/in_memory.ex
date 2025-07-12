defmodule ExDb.Storage.InMemory do
  @moduledoc """
  In-memory storage adapter using ETS (Erlang Term Storage).

  This adapter stores tables and rows in ETS tables for fast concurrent access.
  Each database table gets its own ETS table, and rows are stored with
  auto-incrementing IDs.

  State structure:
  %{
    tables: %{"table_name" => ets_table_ref},
    next_table_id: integer()
  }

  Each ETS table stores rows as: {row_id, [value1, value2, ...]}
  """

  @behaviour ExDb.Storage.Adapter

  @doc """
  Creates initial state for the in-memory adapter.
  """
  def new() do
    %{
      tables: %{},
      next_table_id: 1
    }
  end

  @impl ExDb.Storage.Adapter
  def create_table(state, table_name) when is_binary(table_name) do
    case Map.has_key?(state.tables, table_name) do
      true ->
        {:error, {:table_already_exists, table_name}}

      false ->
        # Create ETS table with unique name
        ets_table_name = :"table_#{state.next_table_id}"
        ets_ref = :ets.new(ets_table_name, [:set, :public, {:keypos, 1}])

        new_state = %{
          state
          | tables: Map.put(state.tables, table_name, ets_ref),
            next_table_id: state.next_table_id + 1
        }

        {:ok, new_state}
    end
  end

  @impl ExDb.Storage.Adapter
  def table_exists?(state, table_name) when is_binary(table_name) do
    Map.has_key?(state.tables, table_name)
  end

  @impl ExDb.Storage.Adapter
  def insert_row(state, table_name, values) when is_binary(table_name) and is_list(values) do
    case Map.get(state.tables, table_name) do
      nil ->
        {:error, {:table_not_found, table_name}}

      ets_ref ->
        # Get next row ID (simple auto-increment)
        row_id = get_next_row_id(ets_ref)

        # Insert row as {row_id, values}
        :ets.insert(ets_ref, {row_id, values})

        {:ok, state}
    end
  end

  @impl ExDb.Storage.Adapter
  def select_all_rows(state, table_name) when is_binary(table_name) do
    case Map.get(state.tables, table_name) do
      nil ->
        {:error, {:table_not_found, table_name}}

      ets_ref ->
        # Get all rows and extract just the values (not the row_id)
        rows =
          :ets.tab2list(ets_ref)
          |> Enum.map(fn {_row_id, values} -> values end)
          |> Enum.sort_by(fn row ->
            # Sort by first column if it exists and is a number
            case row do
              [first | _] when is_number(first) -> first
              _ -> 0
            end
          end)

        {:ok, rows, state}
    end
  end

  @impl ExDb.Storage.Adapter
  def table_info(state, table_name) when is_binary(table_name) do
    case Map.get(state.tables, table_name) do
      nil ->
        {:error, {:table_not_found, table_name}}

      ets_ref ->
        row_count = :ets.info(ets_ref, :size)

        info = %{
          name: table_name,
          type: :table,
          row_count: row_count,
          storage: :ets
        }

        {:ok, info, state}
    end
  end

  # Private helper to get next row ID
  defp get_next_row_id(ets_ref) do
    # Use table size + 1 for next row ID (simple auto-increment)
    :ets.info(ets_ref, :size) + 1
  end
end
