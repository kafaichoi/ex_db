defmodule ExDb.SQL.AST do
  @moduledoc """
  Abstract Syntax Tree definitions for SQL statements.
  """

  defmodule SelectStatement do
    @moduledoc """
    Represents a SELECT statement.
    """
    defstruct [:columns, :from, :where]

    @type t :: %__MODULE__{
      columns: [Column.t()],
      from: Table.t(),
      where: BinaryExpression.t() | nil
    }
  end

  defmodule Column do
    @moduledoc """
    Represents a column reference in a SELECT statement.
    """
    defstruct [:name]

    @type t :: %__MODULE__{
      name: String.t()
    }
  end

  defmodule Table do
    @moduledoc """
    Represents a table reference.
    """
    defstruct [:name]

    @type t :: %__MODULE__{
      name: String.t()
    }
  end

  defmodule Literal do
    @moduledoc """
    Represents a literal value (string, number, boolean).
    """
    defstruct [:type, :value]

    @type literal_type :: :string | :number | :boolean

    @type t :: %__MODULE__{
      type: literal_type(),
      value: any()
    }
  end

  defmodule BinaryExpression do
    @moduledoc """
    Represents a binary expression with left operand, operator, and right operand.
    Used for comparisons (=, <, >, etc.) and logical operations (AND, OR).
    """
    defstruct [:left, :operator, :right]

    @type operand :: Column.t() | Literal.t() | BinaryExpression.t()

    @type t :: %__MODULE__{
      left: operand(),
      operator: String.t(),
      right: operand()
    }
  end

  defmodule Expression do
    @moduledoc """
    Base module for expressions. Currently just an alias for BinaryExpression,
    but can be extended for other expression types in the future.
    """

    @type t :: BinaryExpression.t()
  end
end
