defmodule ExDb.SQL.TokenizerTest do
  use ExUnit.Case, async: true

  alias ExDb.SQL.Tokenizer
  alias ExDb.SQL.Token

  describe "tokenize/1" do
    test "tokenizes basic SELECT statement" do
      sql = "SELECT * FROM users"

      expected = [
        %Token{type: :keyword, value: "SELECT"},
        %Token{type: :operator, value: "*"},
        %Token{type: :keyword, value: "FROM"},
        %Token{type: :identifier, value: "users"}
      ]

      assert Tokenizer.tokenize(sql) == {:ok, expected}
    end

    test "tokenizes SELECT with string literal" do
      sql = "SELECT 'hello'"

      expected = [
        %Token{type: :keyword, value: "SELECT"},
        %Token{type: :literal, value: %Token.Literal{type: :string, value: "hello"}}
      ]

      assert Tokenizer.tokenize(sql) == {:ok, expected}
    end

    test "tokenizes SELECT with number literal" do
      sql = "SELECT 42"

      expected = [
        %Token{type: :keyword, value: "SELECT"},
        %Token{type: :literal, value: %Token.Literal{type: :number, value: 42}}
      ]

      assert Tokenizer.tokenize(sql) == {:ok, expected}
    end

    test "tokenizes identifiers and keywords case-insensitively" do
      sql = "select name from Users"

      expected = [
        %Token{type: :keyword, value: "SELECT"},
        %Token{type: :identifier, value: "name"},
        %Token{type: :keyword, value: "FROM"},
        %Token{type: :identifier, value: "Users"}
      ]

      assert Tokenizer.tokenize(sql) == {:ok, expected}
    end

    test "tokenizes comparison operators" do
      sql = "id = 42"

      expected = [
        %Token{type: :identifier, value: "id"},
        %Token{type: :operator, value: "="},
        %Token{type: :literal, value: %Token.Literal{type: :number, value: 42}}
      ]

      assert Tokenizer.tokenize(sql) == {:ok, expected}
    end

    test "tokenizes multiple comparison operators" do
      sql = "< > <= >= != ="

      expected = [
        %Token{type: :operator, value: "<"},
        %Token{type: :operator, value: ">"},
        %Token{type: :operator, value: "<="},
        %Token{type: :operator, value: ">="},
        %Token{type: :operator, value: "!="},
        %Token{type: :operator, value: "="}
      ]

      assert Tokenizer.tokenize(sql) == {:ok, expected}
    end

    test "handles whitespace correctly" do
      sql = "  SELECT   *   FROM    users  "

      expected = [
        %Token{type: :keyword, value: "SELECT"},
        %Token{type: :operator, value: "*"},
        %Token{type: :keyword, value: "FROM"},
        %Token{type: :identifier, value: "users"}
      ]

      assert Tokenizer.tokenize(sql) == {:ok, expected}
    end

    test "tokenizes comma separator" do
      sql = "column1, column2"

      expected = [
        %Token{type: :identifier, value: "column1"},
        %Token{type: :punctuation, value: ","},
        %Token{type: :identifier, value: "column2"}
      ]

      assert Tokenizer.tokenize(sql) == {:ok, expected}
    end

    test "tokenizes logical operators" do
      sql = "AND OR"

      expected = [
        %Token{type: :keyword, value: "AND"},
        %Token{type: :keyword, value: "OR"}
      ]

      assert Tokenizer.tokenize(sql) == {:ok, expected}
    end

    test "returns error for unterminated string" do
      sql = "SELECT 'unterminated"

      assert Tokenizer.tokenize(sql) == {:error, "Unterminated string literal"}
    end

    test "returns error for invalid character" do
      sql = "SELECT @invalid"

      assert Tokenizer.tokenize(sql) == {:error, "Invalid character: @"}
    end
  end
end
