defmodule ExDb.SQL.Parser do
  @moduledoc """
  Recursive descent parser for SQL statements.
  """

  defstruct [:tokens, :current]

  alias ExDb.SQL.{Tokenizer, Token}

  alias ExDb.SQL.AST.{
    SelectStatement,
    InsertStatement,
    CreateTableStatement,
    ColumnDefinition,
    Table,
    Literal,
    Column,
    BinaryOp
  }

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
        {:ok, []} ->
          {:error, "Empty token list"}

        {:ok, tokens} ->
          parser = %__MODULE__{tokens: tokens, current: 0}
          parse_statement(parser)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # Main statement parsing entry point
  defp parse_statement(parser) do
    case peek(parser) do
      %Token{type: :keyword, value: "SELECT"} ->
        parse_select_statement(parser)

      %Token{type: :keyword, value: "INSERT"} ->
        parse_insert_statement(parser)

      %Token{type: :keyword, value: "CREATE"} ->
        parse_create_table_statement(parser)

      token ->
        {:error, "Unexpected token: #{inspect(token)}"}
    end
  end

  defp peek(%__MODULE__{tokens: tokens, current: current}) do
    if current < length(tokens) do
      Enum.at(tokens, current)
    else
      %Token{type: :eof, value: nil}
    end
  end

  defp parse_select_statement(parser) do
    with {:ok, parser} <- consume(parser, Token.select()),
         {:ok, {columns, parser}} <- parse_select_exprs(parser),
         {:ok, {from_table, parser}} <- parse_optional_from_table(parser),
         {:ok, {where, parser}} <- parse_optional_where(parser) do
      # Validate that all tokens are consumed
      case peek(parser) do
        %Token{type: :eof} ->
          {:ok, %SelectStatement{columns: columns, from: from_table, where: where}}

        _token ->
          cond do
            from_table != nil and where != nil ->
              {:error, "Unexpected tokens after WHERE clause"}

            from_table != nil and where == nil ->
              {:error, "Unexpected tokens after table name"}

            from_table == nil and where == nil ->
              {:error, "Unexpected tokens after SELECT list"}

            true ->
              {:error, "Unexpected tokens after statement"}
          end
      end
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_insert_statement(parser) do
    with {:ok, parser} <- consume(parser, Token.insert()),
         {:ok, parser} <- consume(parser, Token.into()),
         {:ok, {table, parser}} <- parse_table_name(parser),
         {:ok, parser} <- consume(parser, Token.values()),
         {:ok, parser} <- consume(parser, Token.left_paren()),
         {:ok, {values, parser}} <- parse_values_list(parser),
         {:ok, parser} <- consume(parser, Token.right_paren()) do
      # Validate that all tokens are consumed
      case peek(parser) do
        %Token{type: :eof} ->
          {:ok, %InsertStatement{table: table, values: values}}

        _token ->
          {:error, "Unexpected tokens after INSERT statement"}
      end
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_create_table_statement(parser) do
    with {:ok, parser} <- consume(parser, Token.create()),
         {:ok, parser} <- consume(parser, Token.table()),
         {:ok, {table, parser}} <- parse_table_name(parser) do
      # Check for column definitions in parentheses
      case peek(parser) do
        %Token{type: :punctuation, value: "("} ->
          # Parse column definitions
          with {:ok, parser} <- consume(parser, Token.left_paren()),
               {:ok, {columns, parser}} <- parse_column_definitions(parser),
               {:ok, parser} <- consume(parser, Token.right_paren()) do
            # Validate that all tokens are consumed
            case peek(parser) do
              %Token{type: :eof} ->
                {:ok, %CreateTableStatement{table: table, columns: columns}}

              _token ->
                {:error, "Unexpected tokens after column definitions"}
            end
          else
            {:error, reason} ->
              {:error, reason}
          end

        %Token{type: :eof} ->
          # Support legacy syntax without column definitions
          {:ok, %CreateTableStatement{table: table, columns: nil}}

        _token ->
          {:error, "Expected column definitions in parentheses or end of statement"}
      end
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_column_definitions(parser) do
    do_parse_column_definitions(parser, [])
  end

  defp do_parse_column_definitions(parser, columns) do
    with {:ok, {column, parser}} <- parse_column_definition(parser) do
      case consume(parser, Token.comma()) do
        {:ok, parser} ->
          do_parse_column_definitions(parser, [column | columns])

        {:error, _} ->
          {:ok, {Enum.reverse([column | columns]), parser}}
      end
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_column_definition(parser) do
    with {:ok, {column_name, parser}} <- parse_column_name(parser),
         {:ok, {column_type, size, parser}} <- parse_column_type(parser) do
      column_def = %ColumnDefinition{
        name: column_name,
        type: column_type,
        size: size
      }

      {:ok, {column_def, parser}}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_column_name(parser) do
    case advance(parser) do
      {%Token{type: :identifier, value: column_name}, parser} ->
        {:ok, {column_name, parser}}

      {token, _parser} when token != nil ->
        {:error, "Expected column name, got #{token.type}"}

      {nil, _parser} ->
        {:error, "Expected column name"}
    end
  end

  defp parse_column_type(parser) do
    case advance(parser) do
      {%Token{type: :keyword, value: "INTEGER"}, parser} ->
        {:ok, {:integer, nil, parser}}

      {%Token{type: :keyword, value: "TEXT"}, parser} ->
        {:ok, {:text, nil, parser}}

      {%Token{type: :keyword, value: "BOOLEAN"}, parser} ->
        {:ok, {:boolean, nil, parser}}

      {%Token{type: :keyword, value: "VARCHAR"}, parser} ->
        # Handle VARCHAR with optional size specification
        case peek(parser) do
          %Token{type: :punctuation, value: "("} ->
            with {:ok, parser} <- consume(parser, Token.left_paren()),
                 {:ok, {size, parser}} <- parse_varchar_size(parser),
                 {:ok, parser} <- consume(parser, Token.right_paren()) do
              {:ok, {:varchar, size, parser}}
            else
              {:error, reason} ->
                {:error, reason}
            end

          _ ->
            # VARCHAR without size defaults to 255
            {:ok, {:varchar, 255, parser}}
        end

      {token, _parser} when token != nil ->
        {:error, "Expected column type, got #{token.type}"}

      {nil, _parser} ->
        {:error, "Expected column type"}
    end
  end

  defp parse_varchar_size(parser) do
    case advance(parser) do
      {%Token{type: :literal, value: %Token.Literal{type: :number, value: size}}, parser} ->
        {:ok, {size, parser}}

      {token, _parser} when token != nil ->
        {:error, "Expected number for VARCHAR size, got #{token.type}"}

      {nil, _parser} ->
        {:error, "Expected number for VARCHAR size"}
    end
  end

  defp parse_values_list(parser) do
    do_parse_values_list(parser, [])
  end

  defp do_parse_values_list(parser, values) do
    with {:ok, {value, parser}} <- parse_literal_value(parser) do
      case consume(parser, Token.comma()) do
        {:ok, parser} ->
          do_parse_values_list(parser, [value | values])

        {:error, _} ->
          {:ok, {Enum.reverse([value | values]), parser}}
      end
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_literal_value(parser) do
    case advance(parser) do
      {%Token{type: :literal, value: %Token.Literal{type: type, value: value}}, parser} ->
        literal = %Literal{type: type, value: value}
        {:ok, {literal, parser}}

      {token, _parser} when token != nil ->
        {:error, "Expected literal value, got #{token.type}"}

      {nil, _parser} ->
        {:error, "Expected literal value"}
    end
  end

  defp advance(%__MODULE__{tokens: tokens, current: current} = parser) do
    if current < length(tokens) do
      {Enum.at(tokens, current), %{parser | current: current + 1}}
    else
      {nil, parser}
    end
  end

  defp consume(parser, token) do
    case peek(parser) do
      ^token ->
        {:ok, elem(advance(parser), 1)}

      token2 ->
        {:error, "Expected #{inspect(token)} but got #{inspect(token2)}"}
    end
  end

  defp parse_select_exprs(parser) do
    if peek(parser).type == :eof do
      {:error, "Expected column list"}
    else
      do_parse_select_exprs(parser, [])
    end
  end

  defp do_parse_select_exprs(parser, exprs) do
    with {:ok, {expr, parser}} <- parse_expr(parser) do
      case consume(parser, Token.comma()) do
        {:ok, parser} ->
          do_parse_select_exprs(parser, [expr | exprs])

        {:error, _} ->
          {:ok, {Enum.reverse([expr | exprs]), parser}}
      end
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_expr(parser) do
    case advance(parser) do
      {%Token{type: :literal, value: %Token.Literal{type: type, value: value}}, parser} ->
        literal = %Literal{type: type, value: value}
        {:ok, {literal, parser}}

      {%Token{type: :identifier, value: name}, parser} ->
        column = %Column{name: name}
        {:ok, {column, parser}}

      {%Token{type: :operator, value: "*"}, parser} ->
        column = %Column{name: "*"}
        {:ok, {column, parser}}

      {token, _parser} when token != nil ->
        {:error, "Expected identifier, literal, or *, got #{token.type}"}

      {nil, _parser} ->
        {:error, "Expected expression"}
    end
  end

  defp parse_optional_from_table(parser) do
    case peek(parser) do
      %Token{type: :keyword, value: "FROM"} ->
        parse_from_table(parser)

      _ ->
        {:ok, {nil, parser}}
    end
  end

  defp parse_from_table(parser) do
    with {:ok, parser} <- consume(parser, Token.from()),
         {:ok, {table, parser}} <- parse_table_name(parser) do
      {:ok, {table, parser}}
    end
  end

  defp parse_table_name(parser) do
    case advance(parser) do
      {%Token{type: :identifier, value: table_name}, parser} ->
        table = %Table{name: table_name}
        {:ok, {table, parser}}

      {%Token{type: :number}, _parser} ->
        {:error, "Expected table name, got number"}

      {%Token{type: :string}, _parser} ->
        {:error, "Expected table name, got string"}

      {token, _parser} when token != nil ->
        {:error, "Expected table name, got #{token.type}"}

      {nil, _parser} ->
        {:error, "Expected table name after FROM"}
    end
  end

  defp parse_optional_where(parser) do
    case peek(parser) do
      %Token{type: :keyword, value: "WHERE"} ->
        case parse_where_clause(parser) do
          {:ok, {where, parser}} ->
            # Check for invalid logical operators after WHERE expression
            case peek(parser) do
              %Token{type: :identifier, value: _} ->
                {:error, "Expected AND or OR, got identifier"}

              %Token{type: :keyword, value: keyword} when keyword not in ["AND", "OR"] ->
                {:error, "Expected AND or OR, got keyword"}

              _ ->
                {:ok, {where, parser}}
            end

          error ->
            error
        end

      _ ->
        {:ok, {nil, parser}}
    end
  end

  defp parse_where_clause(parser) do
    with {:ok, parser} <- consume(parser, Token.where()),
         {:ok, {expr, parser}} <- parse_where_expression(parser) do
      {:ok, {expr, parser}}
    end
  end

  defp parse_where_expression(parser) do
    with {:ok, {left, parser}} <- parse_primary_expression(parser) do
      case peek(parser) do
        %Token{type: :operator, value: op} when op in ["=", "!=", "<", ">", "<=", ">="] ->
          parse_binary_expression_rest(parser, left, 0)

        %Token{type: :keyword, value: op} when op in ["AND", "OR"] ->
          parse_binary_expression_rest(parser, left, 0)

        %Token{type: :identifier, value: _} ->
          {:error, "Expected operator in expression"}

        %Token{type: :literal, value: _} ->
          {:error, "Expected operator in expression"}

        _ ->
          {:ok, {left, parser}}
      end
    end
  end

  # Binary expression parsing with precedence climbing
  defp parse_binary_expression(parser, min_precedence) do
    with {:ok, {left, parser}} <- parse_primary_expression(parser) do
      parse_binary_expression_rest(parser, left, min_precedence)
    end
  end

  defp parse_binary_expression_rest(parser, left, min_precedence) do
    case peek(parser) do
      %Token{type: :operator, value: op} when op in ["=", "!=", "<", ">", "<=", ">="] ->
        prec = operator_precedence(op)

        if prec >= min_precedence do
          case consume(parser, %Token{type: :operator, value: op}) do
            {:ok, parser} ->
              case parse_binary_expression(parser, prec + 1) do
                {:ok, {right, parser}} ->
                  binary_op = %BinaryOp{left: left, operator: op, right: right}
                  parse_binary_expression_rest(parser, binary_op, min_precedence)

                {:error, _reason} ->
                  {:error, "Expected expression"}
              end

            {:error, reason} ->
              {:error, reason}
          end
        else
          {:ok, {left, parser}}
        end

      %Token{type: :keyword, value: op} when op in ["AND", "OR"] ->
        prec = operator_precedence(op)

        if prec >= min_precedence do
          case consume(parser, %Token{type: :keyword, value: op}) do
            {:ok, parser} ->
              case parse_binary_expression(parser, prec + 1) do
                {:ok, {right, parser}} ->
                  binary_op = %BinaryOp{left: left, operator: op, right: right}
                  parse_binary_expression_rest(parser, binary_op, min_precedence)

                {:error, _reason} ->
                  {:error, "Expected expression"}
              end

            {:error, reason} ->
              {:error, reason}
          end
        else
          {:ok, {left, parser}}
        end

      _ ->
        {:ok, {left, parser}}
    end
  end

  defp parse_primary_expression(parser) do
    parse_expr(parser)
  end

  # Operator precedence (higher number = higher precedence)
  defp operator_precedence("OR"), do: 1
  defp operator_precedence("AND"), do: 2
  defp operator_precedence(op) when op in ["=", "!=", "<", ">", "<=", ">="], do: 3
end
