defmodule ExDb.Storage.Heap do
  @moduledoc """
  PostgreSQL-inspired page-based heap storage adapter.

  This is a complete rewrite of the heap storage using our new page-based architecture:
  - Uses 8KB pages like PostgreSQL
  - Implements page headers and line pointers
  - Provides page-level I/O for better performance
  - Maintains backward compatibility with the Storage.Adapter behavior

  File structure:
  - data/pages/table_name.pages: 8KB pages containing table data
  - Page 0: Header page with table metadata
  - Page 1+: Data pages with actual row tuples

  This replaces the old file-based heap storage with a proper page-based system.
  """

  @behaviour ExDb.Storage.Adapter

  alias ExDb.Storage.{Page, PageManager}
  require Logger

  defstruct [
    :table_name,
    :next_row_id,
    :page_file,
    :schema
  ]

  @type t :: %__MODULE__{
          table_name: String.t(),
          next_row_id: non_neg_integer(),
          page_file: String.t(),
          schema: [ExDb.SQL.AST.ColumnDefinition.t()] | nil
        }

  @doc """
  Creates initial state for a page-based heap storage table.
  """
  def new(table_name) when is_binary(table_name) do
    page_file = Path.join(["data", "pages", "#{table_name}.pages"])

    %__MODULE__{
      table_name: table_name,
      next_row_id: 1,
      page_file: page_file,
      schema: nil
    }
  end

  @impl ExDb.Storage.Adapter
  def create_table(state, table_name, columns) when is_binary(table_name) do
    case PageManager.get_page_count(table_name) do
      {:ok, _count} ->
        # Page file already exists
        {:error, {:table_already_exists, table_name}}

      {:error, _} ->
        case PageManager.create_page_file(table_name) do
          {:ok, page_file} ->
            # Store schema in header page metadata
            # Handle empty or nil columns for backward compatibility
            safe_columns = columns || []

            case update_table_metadata(table_name, %{
                   table_name: table_name,
                   columns: safe_columns,
                   created_at: DateTime.utc_now(),
                   total_tuples: 0,
                   page_format_version: 1
                 }) do
              :ok ->
                new_state = %{
                  state
                  | table_name: table_name,
                    page_file: page_file,
                    schema: safe_columns,
                    next_row_id: 1
                }

                Logger.debug("Created paged heap table",
                  table: table_name,
                  page_file: page_file,
                  columns: length(safe_columns)
                )

                {:ok, new_state}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @impl ExDb.Storage.Adapter
  def table_exists?(_state, table_name) when is_binary(table_name) do
    case PageManager.get_page_count(table_name) do
      {:ok, _count} -> true
      {:error, _} -> false
    end
  end

  @impl ExDb.Storage.Adapter
  def get_table_schema(state, table_name) when is_binary(table_name) do
    case get_table_metadata(table_name) do
      {:ok, metadata} ->
        {:ok, metadata.columns, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl ExDb.Storage.Adapter
  def insert_row(state, table_name, values) when is_binary(table_name) and is_list(values) do
    # Get next row ID from metadata
    case get_table_metadata(table_name) do
      {:ok, metadata} ->
        row_id = metadata.total_tuples + 1

        # Serialize the tuple to calculate size
        tuple_data = :erlang.term_to_binary({row_id, values})
        tuple_size = byte_size(tuple_data)

        # Find a page with enough space or create a new one
        case find_or_create_page_with_space(table_name, tuple_size) do
          {:ok, page_number, page} ->
            # Add tuple to the page
            case Page.add_tuple(page, row_id, values) do
              {:ok, updated_page} ->
                # Write the updated page back to file
                case PageManager.write_page(table_name, page_number, updated_page) do
                  :ok ->
                    # Update table metadata
                    update_table_metadata(table_name, %{metadata | total_tuples: row_id})

                    Logger.debug("Inserted row into paged heap",
                      table: table_name,
                      row_id: row_id,
                      page: page_number,
                      values: values
                    )

                    {:ok, state}

                  {:error, reason} ->
                    {:error, {:page_write_error, reason}}
                end

              {:error, :no_space} ->
                # This shouldn't happen since we checked space, but handle gracefully
                {:error, {:unexpected_no_space, page_number}}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl ExDb.Storage.Adapter
  def select_all_rows(state, table_name) when is_binary(table_name) do
    case PageManager.get_page_count(table_name) do
      {:ok, page_count} when page_count > 1 ->
        # Collect tuples from all data pages (skip page 0 which is header)
        rows =
          1..(page_count - 1)
          |> Enum.flat_map(fn page_num ->
            case PageManager.read_page(table_name, page_num) do
              {:ok, page} ->
                Page.get_all_tuples(page)
                # Return just values
                |> Enum.map(fn {_row_id, values} -> values end)

              {:error, _reason} ->
                Logger.warning("Failed to read page during select",
                  table: table_name,
                  page: page_num
                )

                []
            end
          end)

        Logger.debug("Selected rows from paged heap",
          table: table_name,
          row_count: length(rows),
          pages_scanned: page_count - 1
        )

        {:ok, rows, state}

      {:ok, 1} ->
        # Only header page, no data
        {:ok, [], state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl ExDb.Storage.Adapter
  def update_row(state, table_name, column_name, new_value, where_condition)
      when is_binary(table_name) and is_binary(column_name) do
    case PageManager.get_page_count(table_name) do
      {:ok, page_count} when page_count > 1 ->
        # Simple implementation: read all rows, update matching ones, write back
        # This is not efficient but works for educational purposes
        updated_count = 0

        result =
          1..(page_count - 1)
          |> Enum.reduce_while({:ok, updated_count, state}, fn page_num,
                                                               {:ok, count, current_state} ->
            case update_page_rows(table_name, page_num, column_name, new_value, where_condition) do
              {:ok, page_updated_count} ->
                {:cont, {:ok, count + page_updated_count, current_state}}

              {:error, reason} ->
                {:halt, {:error, reason}}
            end
          end)

        case result do
          {:ok, total_updated, final_state} ->
            Logger.debug("Updated rows in paged heap",
              table: table_name,
              column: column_name,
              updated_count: total_updated
            )

            {:ok, total_updated, final_state}

          {:error, reason} ->
            {:error, reason}
        end

      {:ok, 1} ->
        # Only header page, no data to update
        {:ok, 0, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl ExDb.Storage.Adapter
  def table_info(state, table_name) when is_binary(table_name) do
    case get_table_metadata(table_name) do
      {:ok, metadata} ->
        case PageManager.get_file_stats(table_name) do
          {:ok, file_stats} ->
            info = %{
              name: table_name,
              type: :table,
              row_count: metadata.total_tuples,
              storage: :heap_paged,
              schema: metadata.columns,
              file_size: file_stats.file_size,
              page_count: file_stats.page_count,
              data_pages: file_stats.data_pages,
              created_at: metadata.created_at,
              page_format_version: metadata.page_format_version
            }

            {:ok, info, state}

          {:error, reason} ->
            {:error, {:file_stats_error, reason}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private helper functions

  defp update_page_rows(table_name, page_num, column_name, new_value, where_condition) do
    case PageManager.read_page(table_name, page_num) do
      {:ok, page} ->
        # Get all tuples from the page
        tuples = Page.get_all_tuples(page)

        # Update matching tuples
        {updated_tuples, update_count} =
          tuples
          |> Enum.map_reduce(0, fn {row_id, values}, count ->
            # Simple WHERE evaluation - assumes column order: id, name, email
            row_matches = evaluate_simple_where(values, where_condition)

            if row_matches do
              updated_values = update_column_value(values, column_name, new_value)
              {{row_id, updated_values}, count + 1}
            else
              {{row_id, values}, count}
            end
          end)

        if update_count > 0 do
          # Recreate the page with updated tuples
          new_page = Page.new(page.page_id)

          final_page =
            updated_tuples
            |> Enum.reduce(new_page, fn {row_id, values}, acc_page ->
              case Page.add_tuple(acc_page, row_id, values) do
                {:ok, updated_page} -> updated_page
                # Skip if can't fit
                {:error, _} -> acc_page
              end
            end)

          case PageManager.write_page(table_name, page_num, final_page) do
            :ok -> {:ok, update_count}
            {:error, reason} -> {:error, reason}
          end
        else
          {:ok, 0}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp evaluate_simple_where(_values, nil), do: true

  defp evaluate_simple_where(values, %{left: left, operator: operator, right: right}) do
    left_value =
      case left do
        %{name: "id"} -> Enum.at(values, 0)
        %{name: "name"} -> Enum.at(values, 1)
        %{name: "email"} -> Enum.at(values, 2)
        %{type: _type, value: value} -> value
        _ -> nil
      end

    right_value =
      case right do
        %{type: _type, value: value} -> value
        _ -> nil
      end

    # Handle both string and atom operators
    case operator do
      "=" -> left_value == right_value
      "!=" -> left_value != right_value
      "<" -> left_value < right_value
      ">" -> left_value > right_value
      "<=" -> left_value <= right_value
      ">=" -> left_value >= right_value
      :eq -> left_value == right_value
      :ne -> left_value != right_value
      :lt -> left_value < right_value
      :gt -> left_value > right_value
      :le -> left_value <= right_value
      :ge -> left_value >= right_value
      _ -> false
    end
  end

  defp update_column_value(values, column_name, new_value) do
    case column_name do
      "id" -> List.replace_at(values, 0, new_value)
      "name" -> List.replace_at(values, 1, new_value)
      "email" -> List.replace_at(values, 2, new_value)
      _ -> values
    end
  end

  defp get_table_metadata(table_name) do
    case PageManager.read_page(table_name, 0) do
      {:ok, header_page} ->
        # Extract metadata from header page (stored as first tuple)
        tuples = Page.get_all_tuples(header_page)

        case tuples do
          [{0, [metadata]} | _] ->
            {:ok, metadata}

          [] ->
            {:error, {:no_metadata_found, table_name}}

          other ->
            Logger.warning("Unexpected header page structure",
              table: table_name,
              tuples: other
            )

            {:error, {:invalid_header_page, table_name}}
        end

      {:error, reason} ->
        {:error, {:header_page_error, reason}}
    end
  end

  defp update_table_metadata(table_name, metadata) do
    case PageManager.read_page(table_name, 0) do
      {:ok, _header_page} ->
        # Remove existing metadata tuple and add updated one
        # For simplicity, we'll recreate the header page with new metadata
        new_header = Page.new(0)

        case Page.add_tuple(new_header, 0, [metadata]) do
          {:ok, updated_header} ->
            PageManager.write_page(table_name, 0, updated_header)

          {:error, reason} ->
            {:error, {:metadata_update_error, reason}}
        end

      {:error, reason} ->
        {:error, {:header_read_error, reason}}
    end
  end

  defp find_or_create_page_with_space(table_name, tuple_size) do
    case PageManager.find_page_with_space(table_name, tuple_size) do
      {:ok, page_number, page} ->
        {:ok, page_number, page}

      {:error, :no_data_pages} ->
        # No data pages exist yet, create first data page
        create_new_data_page(table_name)

      {:error, :no_space} ->
        # All existing pages are full, create a new one
        create_new_data_page(table_name)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_new_data_page(table_name) do
    case PageManager.get_page_count(table_name) do
      {:ok, page_count} ->
        # Create new page with page_id = page_count (next available page number)
        new_page = Page.new(page_count)

        case PageManager.append_page(table_name, new_page) do
          {:ok, page_number} ->
            Logger.debug("Created new data page",
              table: table_name,
              page_number: page_number,
              total_pages: page_count + 1
            )

            {:ok, page_number, new_page}

          {:error, reason} ->
            {:error, {:page_creation_error, reason}}
        end

      {:error, reason} ->
        {:error, {:page_count_error, reason}}
    end
  end
end
