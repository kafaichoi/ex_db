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
end
