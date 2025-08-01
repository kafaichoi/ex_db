defmodule ExDb.SQL.Token do
  @moduledoc """
  Represents a token in SQL parsing.
  """

  defstruct [:type, :value]

  @type token_type :: :keyword | :identifier | :operator | :literal | :punctuation

  defmodule Literal do
    defstruct [:type, :value]

    @type t :: %__MODULE__{
            type: :number | :string | :boolean,
            value: number() | String.t() | boolean()
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

  def update() do
    %__MODULE__{type: :keyword, value: "UPDATE"}
  end

  def into() do
    %__MODULE__{type: :keyword, value: "INTO"}
  end

  def values() do
    %__MODULE__{type: :keyword, value: "VALUES"}
  end

  def create() do
    %__MODULE__{type: :keyword, value: "CREATE"}
  end

  def table() do
    %__MODULE__{type: :keyword, value: "TABLE"}
  end

  def integer() do
    %__MODULE__{type: :keyword, value: "INTEGER"}
  end

  def varchar() do
    %__MODULE__{type: :keyword, value: "VARCHAR"}
  end

  def text() do
    %__MODULE__{type: :keyword, value: "TEXT"}
  end

  def boolean() do
    %__MODULE__{type: :keyword, value: "BOOLEAN"}
  end

  def set() do
    %__MODULE__{type: :keyword, value: "SET"}
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
