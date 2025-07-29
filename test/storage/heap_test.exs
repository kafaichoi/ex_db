defmodule ExDb.Storage.HeapTest do
  # File I/O tests need async: false
  use ExUnit.Case, async: false
  alias ExDb.Storage.Heap

  setup do
    # Clean up test data directories before each test
    for dir <- ["data/pages", "data/heap"] do
      if File.exists?(dir) do
        File.rm_rf!(dir)
      end

      File.mkdir_p!(dir)
    end

    state = Heap.new("test_table")
    {:ok, state: state}
  end

  describe "Heap.new/1" do
    test "creates initial state for a paged heap table" do
      state = Heap.new("test_table")

      assert state.table_name == "test_table"
      assert state.next_row_id == 1
      assert String.ends_with?(state.page_file, "test_table.pages")
      assert state.schema == nil
    end
  end

  describe "Heap.create_table/3" do
    test "creates a new paged table with columns", %{state: state} do
      columns = [
        %ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer},
        %ExDb.SQL.AST.ColumnDefinition{name: "name", type: :text}
      ]

      {:ok, new_state} = Heap.create_table(state, "users", columns)

      assert new_state.table_name == "users"
      assert new_state.schema == columns
      assert Heap.table_exists?(new_state, "users") == true
    end

    test "returns error if table already exists", %{state: state} do
      columns = [%ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer}]

      {:ok, _} = Heap.create_table(state, "existing", columns)

      # Try to create again
      result = Heap.create_table(state, "existing", columns)
      assert {:error, {:table_already_exists, "existing"}} = result
    end
  end

  describe "Heap.table_exists?/2" do
    test "returns true for existing table", %{state: state} do
      columns = [%ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer}]
      {:ok, _} = Heap.create_table(state, "test", columns)

      assert Heap.table_exists?(state, "test") == true
    end

    test "returns false for non-existent table", %{state: state} do
      assert Heap.table_exists?(state, "nonexistent") == false
    end
  end

  describe "Heap.get_table_schema/2" do
    test "returns schema for existing table", %{state: state} do
      columns = [
        %ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer},
        %ExDb.SQL.AST.ColumnDefinition{name: "name", type: :text}
      ]

      {:ok, new_state} = Heap.create_table(state, "test", columns)
      {:ok, schema, _state} = Heap.get_table_schema(new_state, "test")

      assert length(schema) == 2
      assert Enum.find(schema, &(&1.name == "id"))
      assert Enum.find(schema, &(&1.name == "name"))
    end

    test "returns error for non-existent table", %{state: state} do
      result = Heap.get_table_schema(state, "nonexistent")
      assert {:error, _} = result
    end
  end

  describe "Heap.insert_row/3" do
    test "inserts a single row", %{state: state} do
      columns = [
        %ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer},
        %ExDb.SQL.AST.ColumnDefinition{name: "name", type: :text}
      ]

      {:ok, state} = Heap.create_table(state, "users", columns)
      {:ok, _state} = Heap.insert_row(state, "users", [1, "Alice"])

      # Verify the row was inserted
      {:ok, rows, _state} = Heap.select_all_rows(state, "users")
      assert length(rows) == 1
      assert [1, "Alice"] in rows
    end

    test "inserts multiple rows", %{state: state} do
      columns = [
        %ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer},
        %ExDb.SQL.AST.ColumnDefinition{name: "name", type: :text}
      ]

      {:ok, state} = Heap.create_table(state, "users", columns)
      {:ok, state} = Heap.insert_row(state, "users", [1, "Alice"])
      {:ok, state} = Heap.insert_row(state, "users", [2, "Bob"])
      {:ok, _state} = Heap.insert_row(state, "users", [3, "Charlie"])

      {:ok, rows, _state} = Heap.select_all_rows(state, "users")
      assert length(rows) == 3
      assert [1, "Alice"] in rows
      assert [2, "Bob"] in rows
      assert [3, "Charlie"] in rows
    end

    test "inserts many rows (testing page splitting)", %{state: state} do
      columns = [
        %ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer},
        %ExDb.SQL.AST.ColumnDefinition{name: "data", type: :text}
      ]

      {:ok, state} = Heap.create_table(state, "big_table", columns)

      # Insert many rows with large data to force multiple pages
      # 500 chars per row
      large_data = String.duplicate("x", 500)

      state =
        1..20
        |> Enum.reduce(state, fn i, acc_state ->
          {:ok, new_state} = Heap.insert_row(acc_state, "big_table", [i, large_data])
          new_state
        end)

      {:ok, rows, _state} = Heap.select_all_rows(state, "big_table")
      assert length(rows) == 20

      # Verify all rows are present
      for i <- 1..20 do
        assert [i, large_data] in rows
      end
    end

    test "returns error for non-existent table", %{state: state} do
      result = Heap.insert_row(state, "nonexistent", [1, "test"])
      assert {:error, _} = result
    end
  end

  describe "Heap.select_all_rows/2" do
    test "returns empty list for table with no rows", %{state: state} do
      columns = [%ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer}]
      {:ok, state} = Heap.create_table(state, "empty", columns)

      {:ok, rows, _state} = Heap.select_all_rows(state, "empty")
      assert rows == []
    end

    test "returns all rows in correct order", %{state: state} do
      columns = [
        %ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer},
        %ExDb.SQL.AST.ColumnDefinition{name: "name", type: :text}
      ]

      {:ok, state} = Heap.create_table(state, "ordered", columns)

      # Insert rows in specific order
      {:ok, state} = Heap.insert_row(state, "ordered", [3, "Charlie"])
      {:ok, state} = Heap.insert_row(state, "ordered", [1, "Alice"])
      {:ok, _state} = Heap.insert_row(state, "ordered", [2, "Bob"])

      {:ok, rows, _state} = Heap.select_all_rows(state, "ordered")
      assert length(rows) == 3

      # Should maintain insertion order (not sorted)
      assert rows == [[3, "Charlie"], [1, "Alice"], [2, "Bob"]]
    end

    test "returns error for non-existent table", %{state: state} do
      result = Heap.select_all_rows(state, "nonexistent")
      assert {:error, _} = result
    end
  end

  describe "Heap.table_info/2" do
    test "returns comprehensive table information", %{state: state} do
      columns = [
        %ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer},
        %ExDb.SQL.AST.ColumnDefinition{name: "name", type: :text}
      ]

      {:ok, state} = Heap.create_table(state, "info_test", columns)
      {:ok, state} = Heap.insert_row(state, "info_test", [1, "Alice"])
      {:ok, _state} = Heap.insert_row(state, "info_test", [2, "Bob"])

      {:ok, info, _state} = Heap.table_info(state, "info_test")

      assert info.name == "info_test"
      assert info.type == :table
      assert info.row_count == 2
      assert info.storage == :heap_paged
      assert info.schema == columns
      assert info.file_size > 0
      assert info.page_count >= 1
      assert info.data_pages >= 0
      assert info.page_format_version == 1
      assert is_struct(info.created_at, DateTime)
    end

    test "returns error for non-existent table", %{state: state} do
      result = Heap.table_info(state, "nonexistent")
      assert {:error, _} = result
    end
  end

  describe "Integration test" do
    test "complete workflow with persistence", %{state: state} do
      # Create table
      columns = [
        %ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer},
        %ExDb.SQL.AST.ColumnDefinition{name: "name", type: :text},
        %ExDb.SQL.AST.ColumnDefinition{name: "email", type: :text}
      ]

      {:ok, state} = Heap.create_table(state, "users", columns)

      # Insert multiple rows
      users = [
        [1, "Alice", "alice@example.com"],
        [2, "Bob", "bob@example.com"],
        [3, "Charlie", "charlie@example.com"]
      ]

      state =
        Enum.reduce(users, state, fn user_data, acc_state ->
          {:ok, new_state} = Heap.insert_row(acc_state, "users", user_data)
          new_state
        end)

      # Verify all data is present
      {:ok, rows, _state} = Heap.select_all_rows(state, "users")
      assert length(rows) == 3

      for user_data <- users do
        assert user_data in rows
      end

      # Verify table info
      {:ok, info, _state} = Heap.table_info(state, "users")
      assert info.row_count == 3
      assert info.storage == :heap_paged
      assert length(info.schema) == 3

      # Test persistence: create new state and verify data is still there
      new_state = Heap.new("users")
      {:ok, persistent_rows, _} = Heap.select_all_rows(new_state, "users")
      assert length(persistent_rows) == 3
      assert persistent_rows == rows
    end
  end

  describe "Storage.Adapter behavior compliance" do
    test "implements all required callbacks", %{state: state} do
      # This test ensures we properly implement the Storage.Adapter behavior
      columns = [%ExDb.SQL.AST.ColumnDefinition{name: "id", type: :integer}]

      # Test all required callbacks work
      assert {:ok, _} = Heap.create_table(state, "behavior_test", columns)
      assert true == Heap.table_exists?(state, "behavior_test")
      assert {:ok, _, _} = Heap.get_table_schema(state, "behavior_test")
      assert {:ok, _} = Heap.insert_row(state, "behavior_test", [1])
      assert {:ok, _, _} = Heap.select_all_rows(state, "behavior_test")
      assert {:ok, _, _} = Heap.table_info(state, "behavior_test")
    end
  end
end
