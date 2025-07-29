defmodule ExDb.Storage.PageManager do
  @moduledoc """
  Manages reading and writing of 8KB pages to files.

  Similar to PostgreSQL's buffer manager but simplified for education.
  This handles the file I/O layer for our page-based storage.

  File structure:
  - Each table has a .pages file containing 8KB pages
  - Pages are numbered starting from 0
  - Page 0 is always the header page with metadata
  - Subsequent pages contain actual table data
  """

  alias ExDb.Storage.Page
  require Logger

  # Must match Page module
  @page_size 8192

  @doc """
  Creates a new page file for a table.
  """
  def create_page_file(table_name) when is_binary(table_name) do
    page_file = get_page_file_path(table_name)

    # Ensure directory exists
    Path.dirname(page_file) |> File.mkdir_p!()

    if File.exists?(page_file) do
      {:error, {:file_already_exists, page_file}}
    else
      # Create header page (page 0) with table metadata
      header_page = create_header_page(table_name)
      header_binary = Page.serialize(header_page)

      case File.write(page_file, header_binary) do
        :ok ->
          Logger.debug("Created page file",
            table: table_name,
            file: page_file,
            initial_pages: 1
          )

          {:ok, page_file}

        {:error, reason} ->
          Logger.error("Failed to create page file",
            table: table_name,
            error: inspect(reason)
          )

          {:error, {:file_write_error, reason}}
      end
    end
  end

  @doc """
  Reads a specific page from a table's page file.
  """
  def read_page(table_name, page_number) when is_binary(table_name) and is_integer(page_number) do
    page_file = get_page_file_path(table_name)

    if not File.exists?(page_file) do
      {:error, {:file_not_found, page_file}}
    else
      offset = page_number * @page_size

      case File.open(page_file, [:read, :binary]) do
        {:ok, file} ->
          result =
            case :file.pread(file, offset, @page_size) do
              {:ok, page_binary} when byte_size(page_binary) == @page_size ->
                try do
                  page = Page.deserialize(page_binary)
                  {:ok, page}
                rescue
                  error ->
                    Logger.error("Failed to deserialize page",
                      table: table_name,
                      page: page_number,
                      error: inspect(error)
                    )

                    {:error, {:deserialization_error, error}}
                end

              {:ok, partial_binary} ->
                Logger.error("Partial page read",
                  table: table_name,
                  page: page_number,
                  expected: @page_size,
                  actual: byte_size(partial_binary)
                )

                {:error, {:partial_read, byte_size(partial_binary)}}

              :eof ->
                {:error, {:page_not_found, page_number}}

              {:error, reason} ->
                {:error, {:read_error, reason}}
            end

          File.close(file)
          result

        {:error, reason} ->
          {:error, {:file_open_error, reason}}
      end
    end
  end

  @doc """
  Writes a page to a specific position in the table's page file.
  """
  def write_page(table_name, page_number, page)
      when is_binary(table_name) and is_integer(page_number) do
    page_file = get_page_file_path(table_name)

    if not File.exists?(page_file) do
      {:error, {:file_not_found, page_file}}
    else
      offset = page_number * @page_size
      page_binary = Page.serialize(page)

      if byte_size(page_binary) != @page_size do
        Logger.error("Invalid page size for write",
          table: table_name,
          page: page_number,
          expected: @page_size,
          actual: byte_size(page_binary)
        )

        {:error, {:invalid_page_size, byte_size(page_binary)}}
      else
        case File.open(page_file, [:read, :write, :binary]) do
          {:ok, file} ->
            result =
              case :file.pwrite(file, offset, page_binary) do
                :ok ->
                  Logger.debug("Wrote page to file",
                    table: table_name,
                    page: page_number,
                    offset: offset
                  )

                  :ok

                {:error, reason} ->
                  Logger.error("Failed to write page",
                    table: table_name,
                    page: page_number,
                    error: inspect(reason)
                  )

                  {:error, {:write_error, reason}}
              end

            File.close(file)
            result

          {:error, reason} ->
            {:error, {:file_open_error, reason}}
        end
      end
    end
  end

  @doc """
  Appends a new page to the end of the table's page file.
  Returns the page number of the newly added page.
  """
  def append_page(table_name, page) when is_binary(table_name) do
    page_file = get_page_file_path(table_name)

    if not File.exists?(page_file) do
      {:error, {:file_not_found, page_file}}
    else
      # Get current page count to determine new page number
      case get_page_count(table_name) do
        {:ok, page_count} ->
          new_page_number = page_count
          page_binary = Page.serialize(page)

          case File.open(page_file, [:append, :binary]) do
            {:ok, file} ->
              result =
                case IO.binwrite(file, page_binary) do
                  :ok ->
                    Logger.debug("Appended new page",
                      table: table_name,
                      page_number: new_page_number,
                      total_pages: page_count + 1
                    )

                    {:ok, new_page_number}

                  {:error, reason} ->
                    Logger.error("Failed to append page",
                      table: table_name,
                      error: inspect(reason)
                    )

                    {:error, {:append_error, reason}}
                end

              File.close(file)
              result

            {:error, reason} ->
              {:error, {:file_open_error, reason}}
          end

        error ->
          error
      end
    end
  end

  @doc """
  Gets the total number of pages in a table's page file.
  """
  def get_page_count(table_name) when is_binary(table_name) do
    page_file = get_page_file_path(table_name)

    case File.stat(page_file) do
      {:ok, %{size: file_size}} ->
        page_count = div(file_size, @page_size)
        {:ok, page_count}

      {:error, :enoent} ->
        {:error, {:file_not_found, page_file}}

      {:error, reason} ->
        {:error, {:stat_error, reason}}
    end
  end

  @doc """
  Finds a page with enough space for a tuple of the given size.
  Returns {:ok, page_number, page} or {:error, :no_space} if no page has space.
  """
  def find_page_with_space(table_name, tuple_size)
      when is_binary(table_name) and is_integer(tuple_size) do
    case get_page_count(table_name) do
      {:ok, page_count} when page_count > 1 ->
        # Skip page 0 (header page), check data pages
        find_space_in_pages(table_name, 1, page_count - 1, tuple_size)

      {:ok, 1} ->
        # Only header page exists, no data pages yet
        {:error, :no_data_pages}

      error ->
        error
    end
  end

  @doc """
  Gets basic statistics about a table's page file.
  """
  def get_file_stats(table_name) when is_binary(table_name) do
    page_file = get_page_file_path(table_name)

    case File.stat(page_file) do
      {:ok, file_stat} ->
        case get_page_count(table_name) do
          {:ok, page_count} ->
            {:ok,
             %{
               file_path: page_file,
               file_size: file_stat.size,
               page_count: page_count,
               # Exclude header page
               data_pages: max(0, page_count - 1),
               created_at: file_stat.ctime,
               modified_at: file_stat.mtime
             }}

          error ->
            error
        end

      {:error, reason} ->
        {:error, {:stat_error, reason}}
    end
  end

  # Private helper functions

  defp get_page_file_path(table_name) do
    Path.join(["data", "pages", "#{table_name}.pages"])
  end

  defp create_header_page(table_name) do
    # Page 0 contains table metadata
    page = Page.new(0)

    # Store table metadata as a special tuple in the header page
    metadata = %{
      table_name: table_name,
      created_at: DateTime.utc_now(),
      page_format_version: 1,
      total_pages: 1,
      total_tuples: 0
    }

    # Add metadata as a tuple with row_id 0 (special system row)
    case Page.add_tuple(page, 0, [metadata]) do
      {:ok, updated_page} -> updated_page
      # If it doesn't fit, return empty page
      {:error, _} -> page
    end
  end

  defp find_space_in_pages(_table_name, page_num, max_page, _tuple_size)
       when page_num > max_page do
    {:error, :no_space}
  end

  defp find_space_in_pages(table_name, page_num, max_page, tuple_size) do
    case read_page(table_name, page_num) do
      {:ok, page} ->
        if Page.has_space_for?(page, tuple_size) do
          {:ok, page_num, page}
        else
          find_space_in_pages(table_name, page_num + 1, max_page, tuple_size)
        end

      {:error, _reason} ->
        # Skip problematic pages and continue searching
        find_space_in_pages(table_name, page_num + 1, max_page, tuple_size)
    end
  end
end
