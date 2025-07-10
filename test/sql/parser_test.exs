defmodule ExDb.SQL.ParserTest do
  use ExUnit.Case, async: true

  alias ExDb.SQL.Parser
  alias ExDb.SQL.AST.{SelectStatement, Column, Table, Literal, BinaryOp}

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
      assert {:error, error} = Parser.parse("INVALID QUERY")
      assert error =~ "Unexpected token: "
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

      assert Parser.parse(sql) == {:error, "Expected table name, got literal"}
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

  describe "parse/1 with basic WHERE clause" do
    test "parses SELECT with simple equality condition" do
      sql = "SELECT * FROM users WHERE id = 42"

      expected = %SelectStatement{
        columns: [%Column{name: "*"}],
        from: %Table{name: "users"},
        where: %BinaryOp{
          left: %Column{name: "id"},
          operator: "=",
          right: %Literal{type: :number, value: 42}
        }
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT with string comparison" do
      sql = "SELECT name FROM users WHERE status = 'active'"

      expected = %SelectStatement{
        columns: [%Column{name: "name"}],
        from: %Table{name: "users"},
        where: %BinaryOp{
          left: %Column{name: "status"},
          operator: "=",
          right: %Literal{type: :string, value: "active"}
        }
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT with greater than condition" do
      sql = "SELECT * FROM users WHERE age > 18"

      expected = %SelectStatement{
        columns: [%Column{name: "*"}],
        from: %Table{name: "users"},
        where: %BinaryOp{
          left: %Column{name: "age"},
          operator: ">",
          right: %Literal{type: :number, value: 18}
        }
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT with all comparison operators" do
      operators = ["=", "!=", "<", ">", "<=", ">="]

      Enum.each(operators, fn op ->
        sql = "SELECT * FROM users WHERE age #{op} 25"

        case Parser.parse(sql) do
          {:ok, %SelectStatement{where: where_expr}} ->
            assert where_expr.left.name == "age"
            assert where_expr.operator == op
            assert where_expr.right.value == 25

          {:error, reason} ->
            flunk("Expected successful parse for operator #{op}, got error: #{reason}")
        end
      end)
    end
  end

  describe "parse/1 with logical operators" do
    test "parses SELECT with AND condition" do
      sql = "SELECT * FROM users WHERE age > 18 AND status = 'active'"

      expected = %SelectStatement{
        columns: [%Column{name: "*"}],
        from: %Table{name: "users"},
        where: %BinaryOp{
          left: %BinaryOp{
            left: %Column{name: "age"},
            operator: ">",
            right: %Literal{type: :number, value: 18}
          },
          operator: "AND",
          right: %BinaryOp{
            left: %Column{name: "status"},
            operator: "=",
            right: %Literal{type: :string, value: "active"}
          }
        }
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT with OR condition" do
      sql = "SELECT * FROM users WHERE status = 'pending' OR status = 'active'"

      expected = %SelectStatement{
        columns: [%Column{name: "*"}],
        from: %Table{name: "users"},
        where: %BinaryOp{
          left: %BinaryOp{
            left: %Column{name: "status"},
            operator: "=",
            right: %Literal{type: :string, value: "pending"}
          },
          operator: "OR",
          right: %BinaryOp{
            left: %Column{name: "status"},
            operator: "=",
            right: %Literal{type: :string, value: "active"}
          }
        }
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "parses SELECT with complex logical expression" do
      sql = "SELECT * FROM users WHERE age > 18 AND status = 'active' OR name = 'admin'"

      # Should parse as: (age > 18 AND status = 'active') OR name = 'admin'
      # Due to AND having higher precedence than OR

      case Parser.parse(sql) do
        {:ok, %SelectStatement{where: where_expr}} ->
          assert where_expr.operator == "OR"
          assert where_expr.left.operator == "AND"
          assert where_expr.right.operator == "="
          assert where_expr.right.left.name == "name"
          assert where_expr.right.right.value == "admin"

        {:error, reason} ->
          flunk("Expected successful parse, got error: #{reason}")
      end
    end
  end

  describe "parse/1 WHERE clause error handling" do
    test "returns error for incomplete WHERE clause" do
      sql = "SELECT * FROM users WHERE"

      assert Parser.parse(sql) == {:error, "Expected expression"}
    end

    test "returns error for invalid WHERE expression" do
      sql = "SELECT * FROM users WHERE ="

      assert {:error, error} = Parser.parse(sql)
      assert error =~ "Expected identifier, literal, "
    end

    test "returns error for missing operator in WHERE" do
      sql = "SELECT * FROM users WHERE id 42"

      assert Parser.parse(sql) == {:error, "Expected operator in expression"}
    end

    test "returns error for incomplete comparison" do
      sql = "SELECT * FROM users WHERE id ="

      assert {:error, error} = Parser.parse(sql)
      assert error =~ "Expected expression"
    end

    test "returns error for invalid logical operator" do
      sql = "SELECT * FROM users WHERE id = 1 INVALID name = 'test'"

      assert Parser.parse(sql) == {:error, "Expected AND or OR, got identifier"}
    end
  end

  describe "parse/1 WHERE clause with whitespace" do
    test "handles extra whitespace in WHERE clause" do
      sql = "SELECT   *   FROM   users   WHERE   id   =   42"

      expected = %SelectStatement{
        columns: [%Column{name: "*"}],
        from: %Table{name: "users"},
        where: %BinaryOp{
          left: %Column{name: "id"},
          operator: "=",
          right: %Literal{type: :number, value: 42}
        }
      }

      assert Parser.parse(sql) == {:ok, expected}
    end

    test "handles case-insensitive WHERE, AND, OR keywords" do
      sql = "select * from users where age > 18 and status = 'active'"

      case Parser.parse(sql) do
        {:ok, %SelectStatement{where: where_expr}} ->
          assert where_expr.operator == "AND"
          assert where_expr.left.operator == ">"
          assert where_expr.right.operator == "="

        {:error, reason} ->
          flunk("Expected successful parse, got error: #{reason}")
      end
    end
  end
end
