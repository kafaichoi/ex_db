defmodule ExDb.Storage.InMemoryServer do
  @moduledoc """
  A GenServer wrapper around InMemory storage to share state across connections.
  """
  use GenServer

  alias ExDb.Storage.InMemory

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    storage_state = InMemory.new()
    {:ok, storage_state}
  end

  # Public API
  def create_table(table_name, columns \\ nil) do
    GenServer.call(__MODULE__, {:create_table, table_name, columns})
  end

  def table_exists?(table_name) do
    GenServer.call(__MODULE__, {:table_exists, table_name})
  end

  def insert_row(table_name, row) do
    GenServer.call(__MODULE__, {:insert_row, table_name, row})
  end

  def select_all_rows(table_name) do
    GenServer.call(__MODULE__, {:select_all_rows, table_name})
  end

  def get_table_schema(table_name) do
    GenServer.call(__MODULE__, {:get_table_schema, table_name})
  end

  def table_info(table_name) do
    GenServer.call(__MODULE__, {:table_info, table_name})
  end

  # GenServer callbacks
  @impl true
  def handle_call({:create_table, table_name, columns}, _from, state) do
    case InMemory.create_table(state, table_name, columns) do
      {:ok, new_state} ->
        {:reply, {:ok, new_state}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:table_exists, table_name}, _from, state) do
    result = InMemory.table_exists?(state, table_name)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:insert_row, table_name, row}, _from, state) do
    case InMemory.insert_row(state, table_name, row) do
      {:ok, new_state} ->
        {:reply, {:ok, new_state}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:select_all_rows, table_name}, _from, state) do
    case InMemory.select_all_rows(state, table_name) do
      {:ok, rows, new_state} ->
        {:reply, {:ok, rows, new_state}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:get_table_schema, table_name}, _from, state) do
    case InMemory.get_table_schema(state, table_name) do
      {:ok, schema, new_state} ->
        {:reply, {:ok, schema, new_state}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:table_info, table_name}, _from, state) do
    case InMemory.table_info(state, table_name) do
      {:ok, info, new_state} ->
        {:reply, {:ok, info, new_state}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end
end
