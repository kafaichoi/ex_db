defmodule ExDb.Storage.HeapTest do
  # Not async due to file I/O
  use ExUnit.Case, async: false

  alias ExDb.Storage.Heap

  # Clean up test data before each test
  setup do
    # Clean up any existing test heap files
    test_data_dir = Path.join(["data", "heap"])

    if File.exists?(test_data_dir) do
      File.rm_rf!(test_data_dir)
    end

    File.mkdir_p!(test_data_dir)

    state = Heap.new("test_table")
    {:ok, state: state}
  end

  describe "create_table/3" do
    test "creates a new table with columns", %{state: state} do
      columns = [
        %{name: "id", type: :integer},
        %{name: "name", type: :text}
      ]

      assert {:ok, new_state} = Heap.create_table(state, "users", columns)
      assert new_state.table_name == "users"
      assert new_state.schema == columns

      # Verify files were created
      assert File.exists?(new_state.heap_file)
      assert File.exists?(new_state.meta_file)
    end

    test "returns error when table already exists", %{state: state} do
      {:ok, state} = Heap.create_table(state, "users", [])

      assert {:error, {:table_already_exists, "users"}} =
               Heap.create_table(state, "users", [])
    end
  end

  describe "table_exists?/2" do
    test "returns true for existing table", %{state: state} do
      {:ok, _state} = Heap.create_table(state, "users", [])

      assert Heap.table_exists?(state, "users") == true
    end

    test "returns false for non-existing table", %{state: state} do
      assert Heap.table_exists?(state, "users") == false
    end
  end

  describe "insert_row/3" do
    test "inserts a row successfully", %{state: state} do
      {:ok, state} = Heap.create_table(state, "users", [])

      assert {:ok, _state} = Heap.insert_row(state, "users", [1, "John", "john@example.com"])
    end

    test "returns error for non-existing table", %{state: state} do
      assert {:error, {:table_not_found, "users"}} =
               Heap.insert_row(state, "users", [1, "John"])
    end
  end

  describe "select_all_rows/2" do
    test "returns empty list for empty table", %{state: state} do
      {:ok, state} = Heap.create_table(state, "users", [])

      assert {:ok, rows, _state} = Heap.select_all_rows(state, "users")
      assert rows == []
    end

    test "returns all rows from table", %{state: state} do
      {:ok, state} = Heap.create_table(state, "users", [])
      {:ok, state} = Heap.insert_row(state, "users", [1, "John", "john@example.com"])
      {:ok, _state} = Heap.insert_row(state, "users", [2, "Jane", "jane@example.com"])

      assert {:ok, rows, _state} = Heap.select_all_rows(state, "users")
      assert length(rows) == 2
      assert [1, "John", "john@example.com"] in rows
      assert [2, "Jane", "jane@example.com"] in rows
    end

    test "returns error for non-existing table", %{state: state} do
      assert {:error, {:table_not_found, "users"}} =
               Heap.select_all_rows(state, "users")
    end
  end

  describe "get_table_schema/2" do
    test "returns table schema", %{state: state} do
      columns = [
        %{name: "id", type: :integer},
        %{name: "name", type: :text}
      ]

      {:ok, state} = Heap.create_table(state, "users", columns)

      assert {:ok, schema, _state} = Heap.get_table_schema(state, "users")
      assert schema == columns
    end

    test "returns error for non-existing table", %{state: state} do
      assert {:error, {:table_not_found, "users"}} =
               Heap.get_table_schema(state, "users")
    end
  end

  describe "table_info/2" do
    test "returns table information", %{state: state} do
      {:ok, state} = Heap.create_table(state, "users", [])
      {:ok, _state} = Heap.insert_row(state, "users", [1, "John"])

      assert {:ok, info, _state} = Heap.table_info(state, "users")
      assert info.name == "users"
      assert info.type == :table
      assert info.row_count == 1
      assert info.storage == :heap
      assert is_integer(info.file_size)
    end

    test "returns error for non-existing table", %{state: state} do
      assert {:error, {:table_not_found, "users"}} =
               Heap.table_info(state, "users")
    end
  end

  describe "persistence across table operations" do
    test "data persists across state recreation", %{state: _state} do
      # Create table and insert data
      state1 = Heap.new("users")
      {:ok, state1} = Heap.create_table(state1, "users", [])
      {:ok, _state1} = Heap.insert_row(state1, "users", [1, "John"])

      # Create new state and verify data still exists
      state2 = Heap.new("users")
      assert {:ok, rows, _state2} = Heap.select_all_rows(state2, "users")
      assert rows == [[1, "John"]]
    end

    test "handles multiple tables independently", %{state: state} do
      {:ok, state} = Heap.create_table(state, "users", [])
      {:ok, state} = Heap.create_table(state, "products", [])

      # Insert into both tables
      {:ok, state} = Heap.insert_row(state, "users", [1, "John"])
      {:ok, state} = Heap.insert_row(state, "products", [1, "Widget", 19.99])

      # Verify each table has its own data
      {:ok, user_rows, state} = Heap.select_all_rows(state, "users")
      {:ok, product_rows, _state} = Heap.select_all_rows(state, "products")

      assert user_rows == [[1, "John"]]
      assert product_rows == [[1, "Widget", 19.99]]
    end
  end

  describe "file format validation" do
    test "handles corrupted heap file gracefully", %{state: state} do
      {:ok, state} = Heap.create_table(state, "users", [])

      # Corrupt the heap file
      File.write!(state.heap_file, "invalid_binary_data")

      # Should return empty list instead of crashing
      assert {:ok, rows, _state} = Heap.select_all_rows(state, "users")
      assert rows == []
    end

    test "handles missing meta file", %{state: state} do
      {:ok, state} = Heap.create_table(state, "users", [])

      # Remove meta file
      File.rm!(state.meta_file)

      # Should return appropriate error
      assert {:error, {:table_not_found, "users"}} =
               Heap.get_table_schema(state, "users")
    end
  end

  describe "row ordering" do
    test "maintains insertion order", %{state: state} do
      {:ok, state} = Heap.create_table(state, "users", [])
      {:ok, state} = Heap.insert_row(state, "users", [3, "Charlie"])
      {:ok, state} = Heap.insert_row(state, "users", [1, "Alice"])
      {:ok, state} = Heap.insert_row(state, "users", [2, "Bob"])

      {:ok, rows, _state} = Heap.select_all_rows(state, "users")

      # Should be in insertion order (heap storage doesn't sort)
      assert rows == [
               [3, "Charlie"],
               [1, "Alice"],
               [2, "Bob"]
             ]
    end
  end

  describe "legacy compatibility" do
    test "create_table/2 works for backward compatibility", %{state: state} do
      assert {:ok, new_state} = Heap.create_table(state, "legacy_table")
      assert new_state.table_name == "legacy_table"
      assert new_state.schema == nil
    end
  end
end
