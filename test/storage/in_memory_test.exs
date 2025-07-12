defmodule ExDb.Storage.InMemoryTest do
  use ExUnit.Case, async: true

  alias ExDb.Storage.InMemory

  setup do
    state = InMemory.new()
    {:ok, state: state}
  end

  describe "new/0" do
    test "creates initial state with empty tables" do
      state = InMemory.new()

      assert state.tables == %{}
      assert state.next_table_id == 1
    end
  end

  describe "create_table/2" do
    test "creates a new table successfully", %{state: state} do
      {:ok, new_state} = InMemory.create_table(state, "users")

      assert Map.has_key?(new_state.tables, "users")
      assert new_state.next_table_id == 2
    end

    test "returns error when table already exists", %{state: state} do
      {:ok, state} = InMemory.create_table(state, "users")

      assert {:error, {:table_already_exists, "users"}} =
               InMemory.create_table(state, "users")
    end
  end

  describe "table_exists?/2" do
    test "returns true for existing table", %{state: state} do
      {:ok, state} = InMemory.create_table(state, "users")

      assert InMemory.table_exists?(state, "users") == true
    end

    test "returns false for non-existing table", %{state: state} do
      assert InMemory.table_exists?(state, "users") == false
    end
  end

  describe "insert_row/3" do
    test "inserts row successfully into existing table", %{state: state} do
      {:ok, state} = InMemory.create_table(state, "users")

      {:ok, _state} = InMemory.insert_row(state, "users", [1, "John", "john@example.com"])
    end

    test "returns error when inserting into non-existing table", %{state: state} do
      assert {:error, {:table_not_found, "users"}} =
               InMemory.insert_row(state, "users", [1, "John"])
    end

    test "allows multiple rows in the same table", %{state: state} do
      {:ok, state} = InMemory.create_table(state, "users")

      {:ok, state} = InMemory.insert_row(state, "users", [1, "John"])
      {:ok, _state} = InMemory.insert_row(state, "users", [2, "Jane"])
    end
  end

  describe "select_all_rows/2" do
    test "returns empty list for table with no rows", %{state: state} do
      {:ok, state} = InMemory.create_table(state, "users")

      {:ok, rows, _state} = InMemory.select_all_rows(state, "users")
      assert rows == []
    end

    test "returns all rows from table", %{state: state} do
      {:ok, state} = InMemory.create_table(state, "users")
      {:ok, state} = InMemory.insert_row(state, "users", [1, "John", "john@example.com"])
      {:ok, state} = InMemory.insert_row(state, "users", [2, "Jane", "jane@example.com"])

      {:ok, rows, _state} = InMemory.select_all_rows(state, "users")

      assert length(rows) == 2
      assert [1, "John", "john@example.com"] in rows
      assert [2, "Jane", "jane@example.com"] in rows
    end

    test "returns error for non-existing table", %{state: state} do
      assert {:error, {:table_not_found, "users"}} =
               InMemory.select_all_rows(state, "users")
    end

    test "rows are sorted by first column when numeric", %{state: state} do
      {:ok, state} = InMemory.create_table(state, "users")
      {:ok, state} = InMemory.insert_row(state, "users", [3, "Charlie"])
      {:ok, state} = InMemory.insert_row(state, "users", [1, "Alice"])
      {:ok, state} = InMemory.insert_row(state, "users", [2, "Bob"])

      {:ok, rows, _state} = InMemory.select_all_rows(state, "users")

      assert rows == [
               [1, "Alice"],
               [2, "Bob"],
               [3, "Charlie"]
             ]
    end
  end

  describe "table_info/2" do
    test "returns info for existing table", %{state: state} do
      {:ok, state} = InMemory.create_table(state, "users")
      {:ok, state} = InMemory.insert_row(state, "users", [1, "John"])

      {:ok, info, _state} = InMemory.table_info(state, "users")

      assert info.name == "users"
      assert info.type == :table
      assert info.row_count == 1
      assert info.storage == :ets
    end

    test "returns error for non-existing table", %{state: state} do
      assert {:error, {:table_not_found, "users"}} =
               InMemory.table_info(state, "users")
    end
  end

  describe "multiple tables" do
    test "can create and manage multiple independent tables", %{state: state} do
      # Create two tables
      {:ok, state} = InMemory.create_table(state, "users")
      {:ok, state} = InMemory.create_table(state, "products")

      # Insert data into each
      {:ok, state} = InMemory.insert_row(state, "users", [1, "John"])
      {:ok, state} = InMemory.insert_row(state, "products", [1, "Widget", 19.99])

      # Verify both tables exist and have correct data
      {:ok, user_rows, state} = InMemory.select_all_rows(state, "users")
      {:ok, product_rows, _state} = InMemory.select_all_rows(state, "products")

      assert user_rows == [[1, "John"]]
      assert product_rows == [[1, "Widget", 19.99]]
    end
  end
end
