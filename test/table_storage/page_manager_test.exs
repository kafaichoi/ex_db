defmodule ExDb.TableStorage.PageManagerTest do
  # File I/O tests need async: false
  use ExUnit.Case, async: false
  alias ExDb.TableStorage.{Page, PageManager}

  setup do
    # Clean up test data directory before each test
    test_data_dir = Path.join(["data", "pages"])

    if File.exists?(test_data_dir) do
      File.rm_rf!(test_data_dir)
    end

    File.mkdir_p!(test_data_dir)

    :ok
  end

  describe "PageManager.create_page_file/1" do
    test "creates a new page file with header page" do
      table_name = "test_table"

      {:ok, page_file} = PageManager.create_page_file(table_name)

      assert File.exists?(page_file)
      assert String.ends_with?(page_file, "test_table.pages")

      # File should be exactly one page (8KB)
      {:ok, file_stat} = File.stat(page_file)
      assert file_stat.size == 8192
    end

    test "returns error if file already exists" do
      table_name = "existing_table"

      {:ok, _} = PageManager.create_page_file(table_name)

      # Try to create again
      result = PageManager.create_page_file(table_name)
      assert {:error, {:file_already_exists, _}} = result
    end
  end

  describe "PageManager.read_page/2" do
    test "reads the header page (page 0)" do
      table_name = "read_test"
      {:ok, _} = PageManager.create_page_file(table_name)

      {:ok, header_page} = PageManager.read_page(table_name, 0)

      assert header_page.page_id == 0
      # May contain metadata
      assert header_page.tuple_count >= 0
    end

    test "returns error for non-existent page" do
      table_name = "read_test"
      {:ok, _} = PageManager.create_page_file(table_name)

      # Try to read page that doesn't exist
      result = PageManager.read_page(table_name, 99)
      assert {:error, {:page_not_found, 99}} = result
    end

    test "returns error for non-existent file" do
      result = PageManager.read_page("nonexistent_table", 0)
      assert {:error, {:file_not_found, _}} = result
    end
  end

  describe "PageManager.write_page/3" do
    test "writes a page to an existing file" do
      table_name = "write_test"
      {:ok, _} = PageManager.create_page_file(table_name)

      # Create a page with some data
      page = Page.new(1)
      {:ok, page_with_data} = Page.add_tuple(page, 100, ["test", "data"])

      # Write to page 1 (not header)
      :ok = PageManager.write_page(table_name, 1, page_with_data)

      # Read it back
      {:ok, read_page} = PageManager.read_page(table_name, 1)

      assert read_page.page_id == 1
      assert read_page.tuple_count == 1

      tuples = Page.get_all_tuples(read_page)
      assert {100, ["test", "data"]} in tuples
    end

    test "returns error for non-existent file" do
      page = Page.new(1)
      result = PageManager.write_page("nonexistent", 1, page)
      assert {:error, {:file_not_found, _}} = result
    end
  end

  describe "PageManager.append_page/2" do
    test "appends a new page to the file" do
      table_name = "append_test"
      {:ok, _} = PageManager.create_page_file(table_name)

      # Initial page count should be 1 (header page)
      {:ok, initial_count} = PageManager.get_page_count(table_name)
      assert initial_count == 1

      # Create and append a new page
      new_page = Page.new(1)
      {:ok, page_with_data} = Page.add_tuple(new_page, 1, ["appended", "data"])

      {:ok, page_number} = PageManager.append_page(table_name, page_with_data)
      # Should be page 1 (after header)
      assert page_number == 1

      # Page count should increase
      {:ok, new_count} = PageManager.get_page_count(table_name)
      assert new_count == 2

      # Verify we can read the appended page
      {:ok, read_page} = PageManager.read_page(table_name, 1)
      tuples = Page.get_all_tuples(read_page)
      assert {1, ["appended", "data"]} in tuples
    end
  end

  describe "PageManager.get_page_count/1" do
    test "returns correct page count" do
      table_name = "count_test"
      {:ok, _} = PageManager.create_page_file(table_name)

      {:ok, count} = PageManager.get_page_count(table_name)
      # Just header page
      assert count == 1

      # Append some pages
      page1 = Page.new(1)
      page2 = Page.new(2)

      {:ok, _} = PageManager.append_page(table_name, page1)
      {:ok, _} = PageManager.append_page(table_name, page2)

      {:ok, new_count} = PageManager.get_page_count(table_name)
      # Header + 2 data pages
      assert new_count == 3
    end

    test "returns error for non-existent file" do
      result = PageManager.get_page_count("nonexistent")
      assert {:error, {:file_not_found, _}} = result
    end
  end

  describe "PageManager.find_page_with_space/2" do
    test "finds a page with available space" do
      table_name = "space_test"
      {:ok, _} = PageManager.create_page_file(table_name)

      # Append a page with some space
      page = Page.new(1)
      {:ok, page_with_data} = Page.add_tuple(page, 1, ["small", "data"])
      {:ok, _} = PageManager.append_page(table_name, page_with_data)

      # Should find space for a small tuple
      small_tuple_size = 100
      {:ok, page_num, found_page} = PageManager.find_page_with_space(table_name, small_tuple_size)

      assert page_num == 1
      assert Page.has_space_for?(found_page, small_tuple_size)
    end

    test "returns no_data_pages for file with only header" do
      table_name = "no_data_test"
      {:ok, _} = PageManager.create_page_file(table_name)

      # Only header page exists, no data pages
      result = PageManager.find_page_with_space(table_name, 100)
      assert {:error, :no_data_pages} = result
    end

    test "returns no_space when no page has enough space" do
      table_name = "full_test"
      {:ok, _} = PageManager.create_page_file(table_name)

      # Create a page filled with data (leaving minimal space)
      page = Page.new(1)
      large_data = String.duplicate("x", 3000)
      {:ok, full_page} = Page.add_tuple(page, 1, [large_data])
      {:ok, _} = PageManager.append_page(table_name, full_page)

      # Try to find space for another large tuple - ensure it's larger than available space
      # Available space is approximately 5148 bytes (8192 - 3016 - 28)
      # This will definitely be too large
      huge_data = String.duplicate("y", 6000)
      serialized_size = byte_size(:erlang.term_to_binary({999, [huge_data]}))
      result = PageManager.find_page_with_space(table_name, serialized_size)
      assert {:error, :no_space} = result
    end
  end

  describe "PageManager.get_file_stats/1" do
    test "returns correct file statistics" do
      table_name = "stats_test"
      {:ok, _} = PageManager.create_page_file(table_name)

      # Add a couple more pages
      page1 = Page.new(1)
      page2 = Page.new(2)
      {:ok, _} = PageManager.append_page(table_name, page1)
      {:ok, _} = PageManager.append_page(table_name, page2)

      {:ok, stats} = PageManager.get_file_stats(table_name)

      assert String.ends_with?(stats.file_path, "stats_test.pages")
      # 3 pages * 8KB
      assert stats.file_size == 3 * 8192
      assert stats.page_count == 3
      # Exclude header page
      assert stats.data_pages == 2
      assert is_tuple(stats.created_at)
      assert is_tuple(stats.modified_at)
    end

    test "returns error for non-existent file" do
      result = PageManager.get_file_stats("nonexistent")
      assert {:error, {:stat_error, :enoent}} = result
    end
  end

  describe "Integration test" do
    test "complete workflow: create, write, read, append" do
      table_name = "integration_test"

      # 1. Create page file
      {:ok, _} = PageManager.create_page_file(table_name)

      # 2. Create a page with data
      page1 = Page.new(1)
      {:ok, page1_with_data} = Page.add_tuple(page1, 100, ["Alice", 25])
      {:ok, page1_final} = Page.add_tuple(page1_with_data, 101, ["Bob", 30])

      # 3. Append the page
      {:ok, page_num} = PageManager.append_page(table_name, page1_final)
      assert page_num == 1

      # 4. Create and append another page
      page2 = Page.new(2)
      {:ok, page2_with_data} = Page.add_tuple(page2, 200, ["Charlie", 35])
      {:ok, _} = PageManager.append_page(table_name, page2_with_data)

      # 5. Read both pages and verify data
      {:ok, read_page1} = PageManager.read_page(table_name, 1)
      {:ok, read_page2} = PageManager.read_page(table_name, 2)

      tuples1 = Page.get_all_tuples(read_page1)
      tuples2 = Page.get_all_tuples(read_page2)

      assert {100, ["Alice", 25]} in tuples1
      assert {101, ["Bob", 30]} in tuples1
      assert {200, ["Charlie", 35]} in tuples2

      # 6. Verify file stats
      {:ok, stats} = PageManager.get_file_stats(table_name)
      # Header + 2 data pages
      assert stats.page_count == 3
      assert stats.data_pages == 2
    end
  end
end
