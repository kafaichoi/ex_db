defmodule ExDb.SQL.Parser do
  @moduledoc """
  Recursive descent parser for SQL statements.
  """

  alias ExDb.SQL.{Tokenizer, Token}
  alias ExDb.SQL.AST.{SelectStatement, Column, Table, Literal}

  @doc """
  Parses a SQL string into an AST.

  ## Examples

      iex> ExDb.SQL.Parser.parse("SELECT 1")
      {:ok, %SelectStatement{columns: [%Literal{type: :number, value: 1}], from: nil, where: nil}}

      iex> ExDb.SQL.Parser.parse("SELECT 'hello'")
      {:ok, %SelectStatement{columns: [%Literal{type: :string, value: "hello"}], from: nil, where: nil}}
  """
  @spec parse(String.t()) :: {:ok, SelectStatement.t()} | {:error, String.t()}
  def parse(sql) when is_binary(sql) do
    sql = String.trim(sql)

    if sql == "" do
      {:error, "Empty query"}
    else
      case Tokenizer.tokenize(sql) do
        {:ok, tokens} ->
          parse_statement(tokens)
        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # Main statement parsing entry point
  defp parse_statement([]), do: {:error, "Empty token list"}

  defp parse_statement([%Token{type: :keyword, value: "SELECT"} | rest]) do
    parse_select_statement(rest)
  end

  defp parse_statement(_tokens) do
    {:error, "Expected SELECT keyword"}
  end

    # Parse SELECT statement
  defp parse_select_statement([]) do
    {:error, "Expected column list"}
  end

  defp parse_select_statement(tokens) do
    case parse_column_list(tokens, []) do
      {:ok, columns, remaining} ->
                case parse_from_clause(remaining) do
          {:ok, from_table, rest} when from_table != nil ->
            statement = %SelectStatement{
              columns: columns,
              from: from_table,
              where: nil
            }

            case rest do
              [] -> {:ok, statement}
              _ -> {:error, "Unexpected tokens after table name"}
            end

          {:ok, nil, []} ->
            # No FROM clause, just literals
            statement = %SelectStatement{
              columns: columns,
              from: nil,
              where: nil
            }
            {:ok, statement}

          {:ok, nil, _rest} ->
            {:error, "Unexpected tokens after SELECT list"}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Parse column list (comma-separated)
  defp parse_column_list([], acc) when acc != [] do
    {:ok, Enum.reverse(acc), []}
  end

  defp parse_column_list([], []) do
    {:error, "Expected column list"}
  end

  defp parse_column_list([%Token{type: :number, value: value} | rest], acc) do
    literal = %Literal{type: :number, value: value}
    parse_column_list_continuation(rest, [literal | acc])
  end

  defp parse_column_list([%Token{type: :string, value: value} | rest], acc) do
    literal = %Literal{type: :string, value: value}
    parse_column_list_continuation(rest, [literal | acc])
  end

  defp parse_column_list([%Token{type: :identifier, value: name} | rest], acc) do
    column = %Column{name: name}
    parse_column_list_continuation(rest, [column | acc])
  end

  defp parse_column_list([%Token{type: :operator, value: "*"} | rest], acc) do
    column = %Column{name: "*"}
    parse_column_list_continuation(rest, [column | acc])
  end

  defp parse_column_list(tokens, _acc) do
    {:error, "Expected column, literal, or wildcard (*), got: #{inspect(List.first(tokens))}"}
  end

    # Handle continuation of column list (comma or end)
  defp parse_column_list_continuation([], acc) do
    {:ok, Enum.reverse(acc), []}
  end

  defp parse_column_list_continuation([%Token{type: :punctuation, value: ","} | rest], acc) do
    # After comma, expect another column
    parse_column_list(rest, acc)
  end

  defp parse_column_list_continuation(remaining, acc) do
    # No comma, so we're done with the column list
    {:ok, Enum.reverse(acc), remaining}
  end

  # Parse FROM clause (optional)
  defp parse_from_clause([]) do
    {:ok, nil, []}
  end

  defp parse_from_clause([%Token{type: :keyword, value: "FROM"} | rest]) do
    parse_table_name(rest)
  end

  defp parse_from_clause(tokens) do
    # No FROM keyword found, return no table and remaining tokens
    {:ok, nil, tokens}
  end

  # Parse table name after FROM
  defp parse_table_name([]) do
    {:error, "Expected table name after FROM"}
  end

  defp parse_table_name([%Token{type: :identifier, value: table_name} | rest]) do
    table = %Table{name: table_name}
    {:ok, table, rest}
  end

  defp parse_table_name([%Token{type: :number, value: _} | _rest]) do
    {:error, "Expected table name, got number"}
  end

  defp parse_table_name([%Token{type: :string, value: _} | _rest]) do
    {:error, "Expected table name, got string"}
  end

  defp parse_table_name([token | _rest]) do
    {:error, "Expected table name, got #{token.type}"}
  end
end
