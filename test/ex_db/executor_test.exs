defmodule ExDb.ExecutorTest do
  use ExUnit.Case, async: true

  alias ExDb.SQL.Parser
  alias ExDb.Storage.InMemory
  alias ExDb.Executor

  @moduletag :integration

  setup do
    # Create fresh storage state for each test
    storage_state = InMemory.new()
    {:ok, storage_state: storage_state}
  end

  describe "end-to-end SQL execution" do
    test "INSERT followed by SELECT returns inserted data", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      # First, we need to create the table (this will be implicit in the future)
      {:ok, storage_state} = InMemory.create_table(storage_state, "users")
      adapter = {InMemory, storage_state}

      # Step 1: Parse and execute INSERT statement
      {:ok, insert_ast} = Parser.parse("INSERT INTO users VALUES (1, 'John', 'john@example.com')")
      {:ok, adapter} = Executor.execute(insert_ast, adapter)

      # Step 2: Parse and execute SELECT statement
      {:ok, select_ast} = Parser.parse("SELECT * FROM users")
      {:ok, result, _adapter} = Executor.execute(select_ast, adapter)

      # Step 3: Verify the complete flow worked
      assert result == [[1, "John", "john@example.com"]]
    end

    test "multiple INSERTs followed by SELECT", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      # Create table
      {:ok, storage_state} = InMemory.create_table(storage_state, "users")
      adapter = {InMemory, storage_state}

      # Insert multiple rows
      {:ok, insert1_ast} = Parser.parse("INSERT INTO users VALUES (1, 'John')")
      {:ok, adapter} = Executor.execute(insert1_ast, adapter)

      {:ok, insert2_ast} = Parser.parse("INSERT INTO users VALUES (2, 'Jane')")
      {:ok, adapter} = Executor.execute(insert2_ast, adapter)

      # Select all data
      {:ok, select_ast} = Parser.parse("SELECT * FROM users")
      {:ok, result, _adapter} = Executor.execute(select_ast, adapter)

      # Should return both rows, sorted by first column
      assert result == [[1, "John"], [2, "Jane"]]
    end

    test "INSERT into non-existing table returns error", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      {:ok, insert_ast} = Parser.parse("INSERT INTO nonexistent VALUES (1, 'test')")

      assert {:error, {:table_not_found, "nonexistent"}} =
               Executor.execute(insert_ast, adapter)
    end

    test "SELECT from non-existing table returns error", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      {:ok, select_ast} = Parser.parse("SELECT * FROM nonexistent")

      assert {:error, {:table_not_found, "nonexistent"}} =
               Executor.execute(select_ast, adapter)
    end

    test "SELECT from empty table returns empty result", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      # Create empty table
      {:ok, storage_state} = InMemory.create_table(storage_state, "users")
      adapter = {InMemory, storage_state}

      {:ok, select_ast} = Parser.parse("SELECT * FROM users")
      {:ok, result, _adapter} = Executor.execute(select_ast, adapter)

      assert result == []
    end

    test "mixed data types in INSERT and SELECT", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      # Create table
      {:ok, storage_state} = InMemory.create_table(storage_state, "products")
      adapter = {InMemory, storage_state}

      # Insert with mixed types (number, string, string)
      {:ok, insert_ast} =
        Parser.parse("INSERT INTO products VALUES (42, 'Widget', 'Electronics')")

      {:ok, adapter} = Executor.execute(insert_ast, adapter)

      # Select and verify
      {:ok, select_ast} = Parser.parse("SELECT * FROM products")
      {:ok, result, _adapter} = Executor.execute(select_ast, adapter)

      assert result == [[42, "Widget", "Electronics"]]
    end
  end

  describe "executor interface design" do
    test "executor returns consistent adapter state tuple", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      {:ok, storage_state} = InMemory.create_table(storage_state, "test")
      adapter = {InMemory, storage_state}

      {:ok, insert_ast} = Parser.parse("INSERT INTO test VALUES (1)")

      # INSERT should return {:ok, adapter}
      assert {:ok, {InMemory, _new_storage_state}} = Executor.execute(insert_ast, adapter)
    end

    test "executor handles different AST types", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      {:ok, storage_state} = InMemory.create_table(storage_state, "test")
      adapter = {InMemory, storage_state}

      # Should handle InsertStatement
      {:ok, insert_ast} = Parser.parse("INSERT INTO test VALUES (1)")
      assert {:ok, _adapter} = Executor.execute(insert_ast, adapter)

      # Should handle SelectStatement
      {:ok, select_ast} = Parser.parse("SELECT * FROM test")
      assert {:ok, _result, _adapter} = Executor.execute(select_ast, adapter)
    end
  end

  describe "CREATE TABLE execution" do
    test "CREATE TABLE creates new table successfully", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      # Parse and execute CREATE TABLE
      {:ok, create_ast} = Parser.parse("CREATE TABLE users")
      {:ok, {InMemory, new_storage_state}} = Executor.execute(create_ast, adapter)

      # Verify table was created
      assert InMemory.table_exists?(new_storage_state, "users") == true
    end

    test "CREATE TABLE allows INSERT and SELECT after creation", %{
      storage_state: storage_state
    } do
      adapter = {InMemory, storage_state}

      # Step 1: Create table using SQL
      {:ok, create_ast} = Parser.parse("CREATE TABLE products")
      {:ok, adapter} = Executor.execute(create_ast, adapter)

      # Step 2: Insert data
      {:ok, insert_ast} = Parser.parse("INSERT INTO products VALUES (1, 'Widget')")
      {:ok, adapter} = Executor.execute(insert_ast, adapter)

      # Step 3: Select data
      {:ok, select_ast} = Parser.parse("SELECT * FROM products")
      {:ok, result, _adapter} = Executor.execute(select_ast, adapter)

      # Step 4: Verify the complete flow worked
      assert result == [[1, "Widget"]]
    end

    test "CREATE TABLE returns error for existing table", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      # Create table manually first
      {:ok, storage_state} = InMemory.create_table(storage_state, "users")
      adapter = {InMemory, storage_state}

      # Try to create the same table again
      {:ok, create_ast} = Parser.parse("CREATE TABLE users")

      assert {:error, {:table_already_exists, "users"}} = Executor.execute(create_ast, adapter)
    end

    test "CREATE TABLE with different table names", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      # Create multiple tables
      {:ok, create1_ast} = Parser.parse("CREATE TABLE users")
      {:ok, adapter} = Executor.execute(create1_ast, adapter)

      {:ok, create2_ast} = Parser.parse("CREATE TABLE products")
      {:ok, adapter} = Executor.execute(create2_ast, adapter)

      # Verify both tables exist
      {_module, storage_state} = adapter
      assert InMemory.table_exists?(storage_state, "users") == true
      assert InMemory.table_exists?(storage_state, "products") == true
    end

    test "CREATE TABLE returns consistent interface", %{storage_state: storage_state} do
      adapter = {InMemory, storage_state}

      {:ok, create_ast} = Parser.parse("CREATE TABLE test")

      # CREATE TABLE should return {:ok, adapter} like INSERT
      assert {:ok, {InMemory, _new_storage_state}} = Executor.execute(create_ast, adapter)
    end
  end
end
