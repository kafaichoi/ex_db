defmodule ExDb.SQL.ASTTest do
  use ExUnit.Case, async: true

  alias ExDb.SQL.AST.{
    SelectStatement,
    Column,
    Table,
    Expression,
    Literal,
    BinaryExpression
  }

  describe "SelectStatement" do
    test "creates basic SELECT statement with columns and table" do
      columns = [
        %Column{name: "id"},
        %Column{name: "name"}
      ]

      table = %Table{name: "users"}

      statement = %SelectStatement{
        columns: columns,
        from: table,
        where: nil
      }

      assert statement.columns == columns
      assert statement.from == table
      assert statement.where == nil
    end

    test "creates SELECT statement with WHERE clause" do
      columns = [%Column{name: "*"}]
      table = %Table{name: "users"}

      where_expr = %BinaryExpression{
        left: %Column{name: "id"},
        operator: "=",
        right: %Literal{type: :number, value: 42}
      }

      statement = %SelectStatement{
        columns: columns,
        from: table,
        where: where_expr
      }

      assert statement.where == where_expr
    end

    test "creates SELECT statement with SELECT * shorthand" do
      statement = %SelectStatement{
        columns: [%Column{name: "*"}],
        from: %Table{name: "products"},
        where: nil
      }

      assert length(statement.columns) == 1
      assert List.first(statement.columns).name == "*"
    end
  end

  describe "Column" do
    test "creates column with simple name" do
      column = %Column{name: "user_id"}

      assert column.name == "user_id"
    end

    test "creates wildcard column" do
      column = %Column{name: "*"}

      assert column.name == "*"
    end
  end

  describe "Table" do
    test "creates table with name" do
      table = %Table{name: "orders"}

      assert table.name == "orders"
    end
  end

  describe "Literal" do
    test "creates string literal" do
      literal = %Literal{type: :string, value: "hello world"}

      assert literal.type == :string
      assert literal.value == "hello world"
    end

    test "creates number literal" do
      literal = %Literal{type: :number, value: 123}

      assert literal.type == :number
      assert literal.value == 123
    end

    test "creates boolean-like literals" do
      # For future boolean support
      literal = %Literal{type: :boolean, value: true}

      assert literal.type == :boolean
      assert literal.value == true
    end
  end

  describe "BinaryExpression" do
    test "creates comparison expression" do
      expr = %BinaryExpression{
        left: %Column{name: "age"},
        operator: ">",
        right: %Literal{type: :number, value: 18}
      }

      assert expr.left.name == "age"
      assert expr.operator == ">"
      assert expr.right.value == 18
    end

    test "creates equality expression" do
      expr = %BinaryExpression{
        left: %Column{name: "status"},
        operator: "=",
        right: %Literal{type: :string, value: "active"}
      }

      assert expr.left.name == "status"
      assert expr.operator == "="
      assert expr.right.value == "active"
    end

    test "creates logical AND expression" do
      left_expr = %BinaryExpression{
        left: %Column{name: "age"},
        operator: ">",
        right: %Literal{type: :number, value: 18}
      }

      right_expr = %BinaryExpression{
        left: %Column{name: "status"},
        operator: "=",
        right: %Literal{type: :string, value: "active"}
      }

      and_expr = %BinaryExpression{
        left: left_expr,
        operator: "AND",
        right: right_expr
      }

      assert and_expr.operator == "AND"
      assert and_expr.left == left_expr
      assert and_expr.right == right_expr
    end

    test "creates logical OR expression" do
      left_expr = %BinaryExpression{
        left: %Column{name: "status"},
        operator: "=",
        right: %Literal{type: :string, value: "pending"}
      }

      right_expr = %BinaryExpression{
        left: %Column{name: "status"},
        operator: "=",
        right: %Literal{type: :string, value: "active"}
      }

      or_expr = %BinaryExpression{
        left: left_expr,
        operator: "OR",
        right: right_expr
      }

      assert or_expr.operator == "OR"
      assert or_expr.left == left_expr
      assert or_expr.right == right_expr
    end
  end

  describe "Expression validation" do
    test "validates supported operators" do
      supported_operators = ["=", "!=", "<", ">", "<=", ">=", "AND", "OR"]

      Enum.each(supported_operators, fn op ->
        expr = %BinaryExpression{
          left: %Column{name: "test"},
          operator: op,
          right: %Literal{type: :string, value: "value"}
        }

        assert expr.operator == op
      end)
    end
  end

  describe "complex expressions" do
    test "creates nested expression tree" do
      # WHERE age > 18 AND (status = 'active' OR status = 'pending')
      age_expr = %BinaryExpression{
        left: %Column{name: "age"},
        operator: ">",
        right: %Literal{type: :number, value: 18}
      }

      status_active = %BinaryExpression{
        left: %Column{name: "status"},
        operator: "=",
        right: %Literal{type: :string, value: "active"}
      }

      status_pending = %BinaryExpression{
        left: %Column{name: "status"},
        operator: "=",
        right: %Literal{type: :string, value: "pending"}
      }

      status_or = %BinaryExpression{
        left: status_active,
        operator: "OR",
        right: status_pending
      }

      final_expr = %BinaryExpression{
        left: age_expr,
        operator: "AND",
        right: status_or
      }

      assert final_expr.operator == "AND"
      assert final_expr.left == age_expr
      assert final_expr.right.operator == "OR"
    end
  end
end
