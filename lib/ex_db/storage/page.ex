defmodule ExDb.Storage.Page do
  @moduledoc """
  PostgreSQL-inspired page structure with 8KB pages.

  Page Layout (8192 bytes total):
  ┌─────────────────────────────────┐  ← Offset 0
  │ Page Header (24 bytes)          │  ← Basic metadata
  ├─────────────────────────────────┤  ← Offset 24
  │ Line Pointers (4 bytes each)    │  ← [offset::16, length::16] for each tuple
  ├─────────────────────────────────┤
  │         Free Space              │  ← Available space
  ├─────────────────────────────────┤
  │ Tuples (stored bottom-up)       │  ← Actual row data
  └─────────────────────────────────┘  ← Offset 8192

  This is similar to PostgreSQL's page structure but simplified for education.
  """

  require Logger

  # Constants (like PostgreSQL)
  # 8KB pages like PostgreSQL
  @page_size 8192
  # Simplified header
  @page_header_size 24
  # offset::16 + length::16
  @line_pointer_size 4

  # Page header structure (24 bytes total)
  # - page_id: unique page identifier (8 bytes)
  # - tuple_count: number of tuples in this page (4 bytes)
  # - free_start: offset where free space starts (4 bytes)
  # - free_end: offset where free space ends (4 bytes)
  # - flags: page flags (2 bytes)
  # - checksum: simple checksum (2 bytes)

  defstruct [
    :page_id,
    :tuple_count,
    # Points to end of line pointer array
    :free_start,
    # Points to start of tuple data (from end)
    :free_end,
    :flags,
    :checksum,
    # List of {offset, length} tuples
    :line_pointers,
    # Binary data containing all tuples
    :tuple_data
  ]

  @type t :: %__MODULE__{
          page_id: non_neg_integer(),
          tuple_count: non_neg_integer(),
          free_start: non_neg_integer(),
          free_end: non_neg_integer(),
          flags: non_neg_integer(),
          checksum: non_neg_integer(),
          line_pointers: [{non_neg_integer(), non_neg_integer()}],
          tuple_data: binary()
        }

  @doc """
  Creates a new empty page with the given page ID.
  """
  def new(page_id) when is_integer(page_id) and page_id >= 0 do
    %__MODULE__{
      page_id: page_id,
      tuple_count: 0,
      # Right after header
      free_start: @page_header_size,
      # At end of page
      free_end: @page_size,
      flags: 0,
      checksum: 0,
      line_pointers: [],
      tuple_data: <<>>
    }
  end

  @doc """
  Adds a tuple (row) to the page if there's enough space.
  Returns {:ok, updated_page} or {:error, :no_space}.
  """
  def add_tuple(page, row_id, values) when is_integer(row_id) and is_list(values) do
    # Serialize tuple like our current format but simpler
    tuple_data = :erlang.term_to_binary({row_id, values})
    tuple_size = byte_size(tuple_data)

    # Calculate space needed: tuple data + one line pointer
    space_needed = tuple_size + @line_pointer_size
    available_space = page.free_end - page.free_start

    if space_needed <= available_space do
      # Calculate new offsets
      new_tuple_offset = page.free_end - tuple_size
      new_free_start = page.free_start + @line_pointer_size
      new_free_end = new_tuple_offset

      # Add line pointer
      new_line_pointer = {new_tuple_offset, tuple_size}
      new_line_pointers = page.line_pointers ++ [new_line_pointer]

      # Add tuple data (concatenate at the end)
      new_tuple_data = page.tuple_data <> tuple_data

      updated_page = %{
        page
        | tuple_count: page.tuple_count + 1,
          free_start: new_free_start,
          free_end: new_free_end,
          line_pointers: new_line_pointers,
          tuple_data: new_tuple_data,
          checksum: calculate_simple_checksum(new_tuple_data)
      }

      Logger.debug("Added tuple to page",
        page_id: page.page_id,
        row_id: row_id,
        tuple_size: tuple_size,
        tuples_in_page: updated_page.tuple_count
      )

      {:ok, updated_page}
    else
      Logger.debug("Page full, cannot add tuple",
        page_id: page.page_id,
        space_needed: space_needed,
        available_space: available_space
      )

      {:error, :no_space}
    end
  end

  @doc """
  Gets all tuples from the page as a list of {row_id, values}.
  """
  def get_all_tuples(page) do
    # Use line pointers to extract individual tuples (like PostgreSQL)
    page.line_pointers
    |> Enum.with_index()
    |> Enum.map(fn {{_offset, length}, index} ->
      # Calculate where this tuple starts in our concatenated data
      tuple_start =
        page.line_pointers
        |> Enum.take(index)
        |> Enum.map(fn {_off, len} -> len end)
        |> Enum.sum()

      tuple_binary = binary_part(page.tuple_data, tuple_start, length)
      :erlang.binary_to_term(tuple_binary, [:safe])
    end)
  end

  @doc """
  Serializes a page to binary format for storage.
  """
  def serialize(page) do
    # Build line pointers binary
    line_pointers_binary =
      page.line_pointers
      |> Enum.map(fn {offset, length} -> <<offset::16, length::16>> end)
      |> Enum.join()

    # Pad line pointers area to maintain alignment
    line_pointers_padding_size =
      page.free_start - @page_header_size - byte_size(line_pointers_binary)

    line_pointers_padding = <<0::size(line_pointers_padding_size * 8)>>

    # Calculate free space padding
    free_space_size = page.free_end - page.free_start
    free_space_padding = <<0::size(free_space_size * 8)>>

    # Build complete page
    header = <<
      page.page_id::64,
      page.tuple_count::32,
      page.free_start::32,
      page.free_end::32,
      page.flags::16,
      page.checksum::16
    >>

    complete_page =
      header <>
        line_pointers_binary <>
        line_pointers_padding <>
        free_space_padding <>
        page.tuple_data

    # Ensure exactly 8KB
    if byte_size(complete_page) != @page_size do
      Logger.error("Page serialization size mismatch",
        expected: @page_size,
        actual: byte_size(complete_page)
      )

      # Pad or truncate to exact size
      padding_needed = @page_size - byte_size(complete_page)

      if padding_needed > 0 do
        complete_page <> <<0::size(padding_needed * 8)>>
      else
        binary_part(complete_page, 0, @page_size)
      end
    else
      complete_page
    end
  end

  @doc """
  Deserializes binary data back into a page structure.
  """
  def deserialize(<<page_binary::binary-size(@page_size)>>) do
    <<
      page_id::64,
      tuple_count::32,
      free_start::32,
      free_end::32,
      flags::16,
      checksum::16,
      rest::binary
    >> = page_binary

    # Extract line pointers
    line_pointers_size = free_start - @page_header_size
    <<line_pointers_binary::binary-size(line_pointers_size), remaining::binary>> = rest

    line_pointers = parse_line_pointers(line_pointers_binary, [])

    # Extract tuple data (skip free space)
    free_space_size = free_end - free_start
    <<_free_space::binary-size(free_space_size), tuple_data::binary>> = remaining

    # Trim tuple_data to actual content (remove padding)
    total_tuple_size =
      line_pointers
      |> Enum.map(fn {_offset, length} -> length end)
      |> Enum.sum()

    actual_tuple_data = binary_part(tuple_data, 0, total_tuple_size)

    %__MODULE__{
      page_id: page_id,
      tuple_count: tuple_count,
      free_start: free_start,
      free_end: free_end,
      flags: flags,
      checksum: checksum,
      line_pointers: line_pointers,
      tuple_data: actual_tuple_data
    }
  end

  @doc """
  Checks if the page has enough space for a tuple of given size.
  """
  def has_space_for?(page, tuple_size) when is_integer(tuple_size) do
    space_needed = tuple_size + @line_pointer_size
    available_space = page.free_end - page.free_start
    space_needed <= available_space
  end

  @doc """
  Gets page statistics for debugging.
  """
  def stats(page) do
    %{
      page_id: page.page_id,
      tuple_count: page.tuple_count,
      free_space: page.free_end - page.free_start,
      utilization: round((@page_size - (page.free_end - page.free_start)) / @page_size * 100),
      checksum: page.checksum
    }
  end

  # Private helper functions

  defp calculate_simple_checksum(binary_data) do
    # Simple checksum: sum of all bytes modulo 65536
    binary_data
    |> :binary.bin_to_list()
    |> Enum.sum()
    |> rem(65536)
  end

  defp parse_line_pointers(<<>>, acc), do: Enum.reverse(acc)

  defp parse_line_pointers(<<offset::16, length::16, rest::binary>>, acc) do
    parse_line_pointers(rest, [{offset, length} | acc])
  end
end
