defmodule ExDb.SQL.Token do
  @moduledoc """
  Represents a token in SQL parsing.
  """

  defstruct [:type, :value]

  @type token_type :: :keyword | :identifier | :operator | :literal | :punctuation

  defmodule Literal do
    defstruct [:type, :value]

    @type t :: %__MODULE__{
            type: :number | :string,
            value: number() | String.t()
          }
  end

  @type t :: %__MODULE__{
          type: token_type(),
          value: Literal.t() | String.t()
        }

  def literal(type, value) do
    %__MODULE__{type: :literal, value: %Literal{type: type, value: value}}
  end

  def greater() do
    %__MODULE__{type: :operator, value: ">"}
  end

  def less() do
    %__MODULE__{type: :operator, value: "<"}
  end

  def equal() do
    %__MODULE__{type: :operator, value: "="}
  end

  def where() do
    %__MODULE__{type: :keyword, value: "WHERE"}
  end

  def select() do
    %__MODULE__{type: :keyword, value: "SELECT"}
  end

  def from() do
    %__MODULE__{type: :keyword, value: "FROM"}
  end

  def insert() do
    %__MODULE__{type: :keyword, value: "INSERT"}
  end

  def into() do
    %__MODULE__{type: :keyword, value: "INTO"}
  end

  def values() do
    %__MODULE__{type: :keyword, value: "VALUES"}
  end

  def left_paren() do
    %__MODULE__{type: :punctuation, value: "("}
  end

  def right_paren() do
    %__MODULE__{type: :punctuation, value: ")"}
  end

  def identifier(value) do
    %__MODULE__{type: :identifier, value: value}
  end

  def string(value) do
    %__MODULE__{type: :string, value: value}
  end

  def comma() do
    %__MODULE__{type: :punctuation, value: ","}
  end

  @doc """
  Represents the end of the token stream.
  Not a real token.
  """
  def eof() do
    %__MODULE__{type: :eof, value: nil}
  end
end
