defmodule ExDb.SQL.Parser do
  @moduledoc """
  Recursive descent parser for SQL statements.
  """

  defstruct [:tokens, :current]

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
      %Token{type: :eof} ->
        {:error, "Unexpected EOF"}

      %Token{type: :keyword, value: "SELECT"} ->
        parse_select_statement(parser)
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
         {:ok, {from_table, parser}} <- parse_from_table(parser),
         {:ok, {where, parser}} <- parse_optional_where(parser) do
      {:ok, %SelectStatement{columns: columns, from: from_table, where: where}}
    else
      {:error, reason} ->
        {:error, reason}
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
    do_parse_select_exprs(parser, [])
  end

  defp do_parse_select_exprs(parser, exprs) do
    with {:ok, {expr, parser}} <- parse_select_expr(parser) do
      case consume(parser, Token.comma()) do
        {:ok, {_, parser}} ->
          do_parse_select_exprs(parser, [expr | exprs])

        {:error, _} ->
          {:ok, {Enum.reverse(exprs), parser}}
      end
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_select_expr(parser) do
    with {%Token{type: type} = expr, parser} when type in [:identifier, :literal] <- advance(parser) do
      {:ok, {expr, parser}}
    else
      _ ->
        {:error, "Expected identifier or literal"}
    end
  end

  defp parse_from_table(parser) do
    with {%Token{type: :keyword, value: "FROM"} = from_token, parser} <- advance(parser),
         {%Token{type: :identifier} = table_token, parser} <- advance(parser) do
      {:ok, {table_token, parser}}
    else
      _ ->
        {:error, "Expected FROM keyword and table name"}
    end
  end

  defp parse_optional_where(parser) do
    case peek(parser) do
      %Token{type: :keyword, value: "WHERE"} ->
        parse_where_clause(parser)

      _ ->
        {:ok, nil, parser}
    end
  end

  defp parse_where_clause(parser) do
    with {:ok, {expr, parser}} <- parse_expr(parser) do
      {:ok, {expr, parser}}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

end
