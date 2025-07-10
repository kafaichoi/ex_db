defmodule ExDb.SQL.Tokenizer do
  @moduledoc """
  Tokenizes SQL strings into a list of tokens.
  """

  alias ExDb.SQL.Token

  @keywords ~w(SELECT FROM WHERE AND OR)
  @operators ~w(= != < > <= >= *)
  @punctuation ~w(,)

  @doc """
  Tokenizes a SQL string into a list of tokens.

  ## Examples

      iex> ExDb.SQL.Tokenizer.tokenize("SELECT * FROM users")
      {:ok, [
        %Token{type: :keyword, value: "SELECT"},
        %Token{type: :operator, value: "*"},
        %Token{type: :keyword, value: "FROM"},
        %Token{type: :identifier, value: "users"}
      ]}
  """
  @spec tokenize(String.t()) :: {:ok, [Token.t()]} | {:error, String.t()}
  def tokenize(sql) when is_binary(sql) do
    sql
    |> String.trim()
    |> do_tokenize([])
  end

  # Main tokenization loop
  defp do_tokenize("", acc), do: {:ok, Enum.reverse(acc)}

  # Skip whitespace
  defp do_tokenize(<<char, rest::binary>>, acc) when char in [?\s, ?\t, ?\n, ?\r] do
    do_tokenize(rest, acc)
  end

  # String literals
  defp do_tokenize(<<"'", rest::binary>>, acc) do
    case extract_string(rest, "") do
      {:ok, value, remaining} ->
        token = %Token{type: :string, value: value}
        do_tokenize(remaining, [token | acc])
      {:error, reason} ->
        {:error, reason}
    end
  end

  # Two-character operators
  defp do_tokenize(<<"<=", rest::binary>>, acc) do
    token = %Token{type: :operator, value: "<="}
    do_tokenize(rest, [token | acc])
  end

  defp do_tokenize(<<">=", rest::binary>>, acc) do
    token = %Token{type: :operator, value: ">="}
    do_tokenize(rest, [token | acc])
  end

  defp do_tokenize(<<"!=", rest::binary>>, acc) do
    token = %Token{type: :operator, value: "!="}
    do_tokenize(rest, [token | acc])
  end

  # Single-character operators and punctuation
  defp do_tokenize(<<char, rest::binary>>, acc) when char in [?=, ?<, ?>, ?*, ?,] do
    token = %Token{type: token_type_for_char(char), value: <<char>>}
    do_tokenize(rest, [token | acc])
  end

  # Numbers
  defp do_tokenize(<<char, _::binary>> = input, acc) when char in ?0..?9 do
    case extract_number(input, "") do
      {:ok, value, remaining} ->
        token = %Token{type: :number, value: value}
        do_tokenize(remaining, [token | acc])
      {:error, reason} ->
        {:error, reason}
    end
  end

  # Identifiers and keywords
  defp do_tokenize(<<char, _::binary>> = input, acc) when char in ?a..?z or char in ?A..?Z or char == ?_ do
    case extract_identifier(input, "") do
      {:ok, value, remaining} ->
        token = %Token{type: token_type_for_word(value), value: normalize_keyword(value)}
        do_tokenize(remaining, [token | acc])
      {:error, reason} ->
        {:error, reason}
    end
  end

  # Invalid characters
  defp do_tokenize(<<char, _::binary>>, _acc) do
    {:error, "Invalid character: #{<<char>>}"}
  end

  # Extract string literal
  defp extract_string("", _acc), do: {:error, "Unterminated string literal"}

  defp extract_string(<<"'", rest::binary>>, acc) do
    {:ok, acc, rest}
  end

  defp extract_string(<<char, rest::binary>>, acc) do
    extract_string(rest, acc <> <<char>>)
  end

  # Extract number
  defp extract_number(<<char, rest::binary>>, acc) when char in ?0..?9 do
    extract_number(rest, acc <> <<char>>)
  end

  defp extract_number(input, acc) do
    case Integer.parse(acc) do
      {value, ""} -> {:ok, value, input}
      _ -> {:error, "Invalid number format"}
    end
  end

  # Extract identifier
  defp extract_identifier(<<char, rest::binary>>, acc) when char in ?a..?z or char in ?A..?Z or char in ?0..?9 or char == ?_ do
    extract_identifier(rest, acc <> <<char>>)
  end

  defp extract_identifier(input, acc) do
    {:ok, acc, input}
  end

  # Determine token type for single character
  defp token_type_for_char(?,), do: :punctuation
  defp token_type_for_char(_), do: :operator

  # Determine if word is keyword or identifier
  defp token_type_for_word(word) do
    if String.upcase(word) in @keywords do
      :keyword
    else
      :identifier
    end
  end

  # Normalize keywords to uppercase
  defp normalize_keyword(word) do
    if String.upcase(word) in @keywords do
      String.upcase(word)
    else
      word
    end
  end
end
