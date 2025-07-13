defmodule ExDb.Storage.SharedInMemory do
  @moduledoc """
  A shared storage adapter that uses InMemoryServer to maintain state across connections.
  """

  @behaviour ExDb.Storage.Adapter

  alias ExDb.Storage.InMemoryServer

  @impl true
  def create_table(state, table_name, columns \\ nil) do
    # The state is ignored since we use the shared GenServer
    case InMemoryServer.create_table(table_name, columns) do
      {:ok, _new_state} ->
        {:ok, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def table_exists?(_state, table_name) do
    # The state is ignored since we use the shared GenServer
    InMemoryServer.table_exists?(table_name)
  end

  @impl true
  def insert_row(state, table_name, row) do
    # The state is ignored since we use the shared GenServer
    case InMemoryServer.insert_row(table_name, row) do
      {:ok, _new_state} ->
        {:ok, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def select_all_rows(state, table_name) do
    # The state is ignored since we use the shared GenServer
    case InMemoryServer.select_all_rows(table_name) do
      {:ok, rows, _new_state} ->
        {:ok, rows, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def get_table_schema(state, table_name) do
    # The state is ignored since we use the shared GenServer
    case InMemoryServer.get_table_schema(table_name) do
      {:ok, schema, _new_state} ->
        {:ok, schema, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def table_info(state, table_name) do
    # The state is ignored since we use the shared GenServer
    case InMemoryServer.table_info(table_name) do
      {:ok, info, _new_state} ->
        {:ok, info, state}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
