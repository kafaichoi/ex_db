defmodule ExDb.SQL.ParserTest do
  use ExUnit.Case, async: true

  alias ExDb.SQL.Parser
  alias ExDb.SQL.AST.{SelectStatement, Column, Table, Literal}

  describe "parse/1 with literal SELECT statements" do
    test "parses SELECT with number literal" do
      sql = "SELECT 1"

      expected = %SelectStatement{
        columns: [%Literal{type: :number, value: 1}],
        from: nil,
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT with string literal" do
      sql = "SELECT 'hello'"

      expected = %SelectStatement{
        columns: [%Literal{type: :string, value: "hello"}],
        from: nil,
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT with multiple literals" do
      sql = "SELECT 42, 'world'"

      expected = %SelectStatement{
        columns: [
          %Literal{type: :number, value: 42},
          %Literal{type: :string, value: "world"}
        ],
        from: nil,
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT with mixed literals and expressions" do
      sql = "SELECT 1, 'test', 99"

      expected = %SelectStatement{
        columns: [
          %Literal{type: :number, value: 1},
          %Literal{type: :string, value: "test"},
          %Literal{type: :number, value: 99}
        ],
        from: nil,
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end
  end

  describe "parse/1 error handling" do
    test "returns error for empty string" do
      assert Parser.parse("") == {:error, "Empty query"}
    end

    test "returns error for whitespace only" do
      assert Parser.parse("   ") == {:error, "Empty query"}
    end

    test "returns error for invalid SQL" do
      assert Parser.parse("INVALID QUERY") == {:error, "Expected SELECT keyword"}
    end

    test "returns error for incomplete SELECT" do
      assert Parser.parse("SELECT") == {:error, "Expected column list"}
    end

    test "returns error for unterminated string in SELECT" do
      sql = "SELECT 'unterminated"
      assert Parser.parse(sql) == {:error, "Unterminated string literal"}
    end
  end

  describe "parse/1 with tokenization integration" do
    test "handles extra whitespace correctly" do
      sql = "  SELECT   42  ,  'hello'  "

      expected = %SelectStatement{
        columns: [
          %Literal{type: :number, value: 42},
          %Literal{type: :string, value: "hello"}
        ],
        from: nil,
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "handles case-insensitive keywords" do
      sql = "select 123"

      expected = %SelectStatement{
        columns: [%Literal{type: :number, value: 123}],
        from: nil,
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end
  end

  describe "integration test: SQL string → tokens → AST" do
    test "complete parsing pipeline for simple literal" do
      sql = "SELECT 42"

      # This tests the entire pipeline:
      # 1. SQL string gets tokenized
      # 2. Tokens get parsed into AST
      # 3. Result is proper SelectStatement

      case Parser.parse(sql) do
        {:ok, %SelectStatement{} = statement} ->
          assert statement.columns == [%Literal{type: :number, value: 42}]
          assert statement.from == nil
          assert statement.where == nil

        {:error, reason} ->
          flunk("Expected successful parse, got error: #{reason}")
      end
    end

    test "complete parsing pipeline for multiple literals" do
      sql = "SELECT 1, 'test', 999"

      case Parser.parse(sql) do
        {:ok, %SelectStatement{} = statement} ->
          assert length(statement.columns) == 3
          assert Enum.at(statement.columns, 0) == %Literal{type: :number, value: 1}
          assert Enum.at(statement.columns, 1) == %Literal{type: :string, value: "test"}
          assert Enum.at(statement.columns, 2) == %Literal{type: :number, value: 999}

        {:error, reason} ->
          flunk("Expected successful parse, got error: #{reason}")
      end
    end
  end

  describe "parse/1 with FROM clause" do
    test "parses SELECT column FROM table" do
      sql = "SELECT name FROM users"

      expected = %SelectStatement{
        columns: [%Column{name: "name"}],
        from: %Table{name: "users"},
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT * FROM table" do
      sql = "SELECT * FROM products"

      expected = %SelectStatement{
        columns: [%Column{name: "*"}],
        from: %Table{name: "products"},
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT multiple columns FROM table" do
      sql = "SELECT id, name, email FROM users"

      expected = %SelectStatement{
        columns: [
          %Column{name: "id"},
          %Column{name: "name"},
          %Column{name: "email"}
        ],
        from: %Table{name: "users"},
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT with mixed columns and literals FROM table" do
      sql = "SELECT id, 'constant', name FROM orders"

      expected = %SelectStatement{
        columns: [
          %Column{name: "id"},
          %Literal{type: :string, value: "constant"},
          %Column{name: "name"}
        ],
        from: %Table{name: "orders"},
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "handles case-insensitive FROM keyword" do
      sql = "SELECT id from Users"

      expected = %SelectStatement{
        columns: [%Column{name: "id"}],
        from: %Table{name: "Users"},
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "handles extra whitespace around FROM" do
      sql = "SELECT   id  ,  name   FROM    users  "

      expected = %SelectStatement{
        columns: [
          %Column{name: "id"},
          %Column{name: "name"}
        ],
        from: %Table{name: "users"},
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end
  end

  describe "parse/1 FROM clause error handling" do
    test "returns error for missing table name after FROM" do
      sql = "SELECT id FROM"

      assert Parser.parse(sql) == {:error, "Expected table name after FROM"}
    end

    test "returns error for invalid token after FROM" do
      sql = "SELECT id FROM 123"

      assert Parser.parse(sql) == {:error, "Expected table name, got number"}
    end

    test "returns error for extra tokens after table name" do
      sql = "SELECT id FROM users extra"

      assert Parser.parse(sql) == {:error, "Unexpected tokens after table name"}
    end

    test "returns error for missing FROM keyword with table-like structure" do
      # This should fail because we expect either just literals or FROM clause
      sql = "SELECT id users"

      assert Parser.parse(sql) == {:error, "Unexpected tokens after SELECT list"}
    end
  end

  describe "parse/1 backwards compatibility" do
    test "still supports SELECT literals without FROM" do
      sql = "SELECT 42, 'hello'"

      expected = %SelectStatement{
        columns: [
          %Literal{type: :number, value: 42},
          %Literal{type: :string, value: "hello"}
        ],
        from: nil,
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "still supports single SELECT literal" do
      sql = "SELECT 1"

      expected = %SelectStatement{
        columns: [%Literal{type: :number, value: 1}],
        from: nil,
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end
  end

  describe "parse/1 edge cases with FROM" do
    test "handles table names with underscores" do
      sql = "SELECT * FROM user_profiles"

      expected = %SelectStatement{
        columns: [%Column{name: "*"}],
        from: %Table{name: "user_profiles"},
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "handles table names with numbers" do
      sql = "SELECT * FROM table123"

      expected = %SelectStatement{
        columns: [%Column{name: "*"}],
        from: %Table{name: "table123"},
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "preserves column name case sensitivity" do
      sql = "SELECT UserID, FirstName FROM Users"

      expected = %SelectStatement{
        columns: [
          %Column{name: "UserID"},
          %Column{name: "FirstName"}
        ],
        from: %Table{name: "Users"},
        where: nil
      }

      assert Parser.parse(sql) == {:ok, expected}
    end
  end
end
