defmodule ExDb.Storage.Heap do
  @moduledoc """
  Heap storage adapter using file-based persistence.

  This adapter stores tables as heap files on disk, similar to PostgreSQL's heap storage.
  For now, this is a simple append-only implementation that will be enhanced with
  pages, shared buffers, and other optimizations later.

  File structure:
  - data/heap/table_name.heap: Table data (rows)
  - data/heap/table_name.meta: Table metadata (schema)

  Row format in heap file:
  [row_id][row_length][column1_data][column2_data]...

  State structure:
  %{
    table_name: "table_name",
    heap_file: "data/heap/table_name.heap",
    meta_file: "data/heap/table_name.meta",
    schema: [%ExDb.SQL.AST.ColumnDefinition{}, ...],
    next_row_id: integer()
  }
  """

  @behaviour ExDb.Storage.Adapter

  require Logger

  @doc """
  Creates initial state for a heap storage table.
  """
  def new(table_name) when is_binary(table_name) do
    heap_file = Path.join(["data", "heap", "#{table_name}.heap"])
    meta_file = Path.join(["data", "heap", "#{table_name}.meta"])

    %{
      table_name: table_name,
      heap_file: heap_file,
      meta_file: meta_file,
      schema: nil,
      next_row_id: 1
    }
  end

  @impl ExDb.Storage.Adapter
  def create_table(state, table_name, columns) when is_binary(table_name) do
    heap_file = Path.join(["data", "heap", "#{table_name}.heap"])
    meta_file = Path.join(["data", "heap", "#{table_name}.meta"])

    # Ensure data/heap directory exists
    Path.dirname(heap_file) |> File.mkdir_p!()

    # Check if table already exists
    if File.exists?(heap_file) do
      {:error, {:table_already_exists, table_name}}
    else
      # Create empty heap file
      File.write!(heap_file, "")

      # Store schema in meta file
      meta_data = %{
        table_name: table_name,
        columns: columns,
        created_at: DateTime.utc_now(),
        row_count: 0
      }

      File.write!(meta_file, :erlang.term_to_binary(meta_data))

      new_state = %{
        state
        | table_name: table_name,
          heap_file: heap_file,
          meta_file: meta_file,
          schema: columns,
          next_row_id: 1
      }

      Logger.debug("Created heap table",
        table: table_name,
        heap_file: heap_file
      )

      {:ok, new_state}
    end
  end

  @impl ExDb.Storage.Adapter
  def table_exists?(_state, table_name) when is_binary(table_name) do
    heap_file = Path.join(["data", "heap", "#{table_name}.heap"])
    File.exists?(heap_file)
  end

  @impl ExDb.Storage.Adapter
  def get_table_schema(state, table_name) when is_binary(table_name) do
    meta_file = Path.join(["data", "heap", "#{table_name}.meta"])

    case File.read(meta_file) do
      {:ok, binary_data} ->
        meta_data = :erlang.binary_to_term(binary_data)
        {:ok, meta_data.columns, state}

      {:error, :enoent} ->
        {:error, {:table_not_found, table_name}}

      {:error, reason} ->
        {:error, {:file_error, reason}}
    end
  end

  @impl ExDb.Storage.Adapter
  def insert_row(state, table_name, values) when is_binary(table_name) and is_list(values) do
    heap_file = Path.join(["data", "heap", "#{table_name}.heap"])

    if not File.exists?(heap_file) do
      {:error, {:table_not_found, table_name}}
    else
      # Load current row count to get next row ID
      {:ok, current_row_id} = get_next_row_id(table_name)

      # Serialize row: [row_id, values]
      row_data = {current_row_id, values}
      binary_row = :erlang.term_to_binary(row_data)
      row_length = byte_size(binary_row)

      # Append to heap file: [length][binary_row]
      file_entry = <<row_length::32, binary_row::binary>>

      case File.open(heap_file, [:append, :binary]) do
        {:ok, file} ->
          result = IO.binwrite(file, file_entry)
          File.close(file)

          case result do
            :ok ->
              # Update row count in meta file
              update_row_count(table_name)

              Logger.debug("Inserted row into heap",
                table: table_name,
                row_id: current_row_id,
                values: values
              )

              {:ok, state}

            {:error, reason} ->
              {:error, {:write_error, reason}}
          end

        {:error, reason} ->
          {:error, {:file_error, reason}}
      end
    end
  end

  @impl ExDb.Storage.Adapter
  def select_all_rows(state, table_name) when is_binary(table_name) do
    heap_file = Path.join(["data", "heap", "#{table_name}.heap"])

    case File.read(heap_file) do
      {:ok, binary_data} ->
        rows = parse_heap_file(binary_data)

        Logger.debug("Selected rows from heap",
          table: table_name,
          row_count: length(rows)
        )

        {:ok, rows, state}

      {:error, :enoent} ->
        {:error, {:table_not_found, table_name}}

      {:error, reason} ->
        {:error, {:file_error, reason}}
    end
  end

  @impl ExDb.Storage.Adapter
  def table_info(state, table_name) when is_binary(table_name) do
    meta_file = Path.join(["data", "heap", "#{table_name}.meta"])

    case File.read(meta_file) do
      {:ok, binary_data} ->
        meta_data = :erlang.binary_to_term(binary_data)

        # Get current file size for storage info
        heap_file = Path.join(["data", "heap", "#{table_name}.heap"])

        file_size =
          case File.stat(heap_file) do
            {:ok, %{size: size}} -> size
            _ -> 0
          end

        info = %{
          name: table_name,
          type: :table,
          row_count: meta_data.row_count,
          storage: :heap,
          schema: meta_data.columns,
          file_size: file_size,
          created_at: meta_data.created_at
        }

        {:ok, info, state}

      {:error, :enoent} ->
        {:error, {:table_not_found, table_name}}

      {:error, reason} ->
        {:error, {:file_error, reason}}
    end
  end

  # Private helper functions

  defp get_next_row_id(table_name) do
    meta_file = Path.join(["data", "heap", "#{table_name}.meta"])

    case File.read(meta_file) do
      {:ok, binary_data} ->
        meta_data = :erlang.binary_to_term(binary_data)
        {:ok, meta_data.row_count + 1}

      {:error, _} ->
        {:ok, 1}
    end
  end

  defp update_row_count(table_name) do
    meta_file = Path.join(["data", "heap", "#{table_name}.meta"])

    case File.read(meta_file) do
      {:ok, binary_data} ->
        meta_data = :erlang.binary_to_term(binary_data)
        updated_meta = %{meta_data | row_count: meta_data.row_count + 1}
        File.write!(meta_file, :erlang.term_to_binary(updated_meta))

      {:error, reason} ->
        Logger.warning("Failed to update row count",
          table: table_name,
          error: inspect(reason)
        )
    end
  end

  defp parse_heap_file(<<>>), do: []

  defp parse_heap_file(<<row_length::32, binary_row::binary-size(row_length), rest::binary>>) do
    {_row_id, values} = :erlang.binary_to_term(binary_row)
    [values | parse_heap_file(rest)]
  end

  defp parse_heap_file(_invalid_data) do
    Logger.error("Invalid heap file format detected")
    []
  end

  # Legacy create_table function for backward compatibility
  def create_table(state, table_name) when is_binary(table_name) do
    create_table(state, table_name, nil)
  end
end
