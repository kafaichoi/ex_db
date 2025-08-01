defmodule ExDb.SQL.AST do
  @moduledoc """
  Abstract Syntax Tree definitions for SQL statements.
  """

  defmodule Column do
    @moduledoc """
    Represents a column reference in a SELECT statement.
    """
    defstruct [:name]

    @type t :: %__MODULE__{
            name: String.t()
          }
  end

  defmodule ColumnDefinition do
    @moduledoc """
    Represents a column definition in a CREATE TABLE statement.
    """
    defstruct [:name, :type, :size]

    @type column_type :: :integer | :varchar | :text | :boolean

    @type t :: %__MODULE__{
            name: String.t(),
            type: column_type(),
            size: integer() | nil
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

  defmodule BinaryOp do
    @moduledoc """
    Represents a binary expression with left operand, operator, and right operand.
    Used for comparisons (=, <, >, etc.) and logical operations (AND, OR).
    """
    defstruct [:left, :operator, :right]

    @type t :: %__MODULE__{
            left: Expression.t(),
            operator: AST.binary_operator(),
            right: Expression.t()
          }
  end

  @type binary_operator :: :eq | :ne | :lt | :le | :gt | :ge | :and | :or
  @type expression :: Column.t() | Literal.t() | BinaryOp.t()

  defmodule SelectStatement do
    @moduledoc """
    Represents a SELECT statement.
    """
    defstruct [:columns, :from, :where]

    @type t :: %__MODULE__{
            columns: [Column.t()],
            from: Table.t(),
            where: BinaryOp.t() | nil
          }
  end

  defmodule InsertStatement do
    @moduledoc """
    Represents an INSERT statement.
    """
    defstruct [:table, :values]

    @type t :: %__MODULE__{
            table: Table.t(),
            values: [Literal.t()]
          }
  end

  defmodule UpdateStatement do
    @moduledoc """
    Represents an UPDATE statement.
    """
    defstruct [:table, :set, :where]

    @type t :: %__MODULE__{
            table: Table.t(),
            set: [%{column: Column.t(), value: Literal.t()}],
            where: BinaryOp.t() | nil
          }
  end

  defmodule CreateTableStatement do
    @moduledoc """
    Represents a CREATE TABLE statement.
    """
    defstruct [:table, :columns]

    @type t :: %__MODULE__{
            table: Table.t(),
            columns: [ColumnDefinition.t()] | nil
          }
  end
end
