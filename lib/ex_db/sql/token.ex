defmodule ExDb.SQL.Token do
  @moduledoc """
  Represents a token in SQL parsing.
  """

  defstruct [:type, :value]

  @type token_type :: :keyword | :identifier | :operator | :string | :number | :punctuation

  @type t :: %__MODULE__{
          type: token_type(),
          value: any()
        }

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

  def identifier(value) do
    %__MODULE__{type: :identifier, value: value}
  end

  def string(value) do
    %__MODULE__{type: :string, value: value}
  end
end
