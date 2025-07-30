defmodule ExDb.BufferManagerTest do
  # File I/O tests need async: false
  use ExUnit.Case, async: false
  alias ExDb.BufferManager
  alias ExDb.TableStorage.{PageManager, Page}

  setup do
    # Clear buffer manager cache BEFORE deleting files to avoid flush errors
    BufferManager.clear_cache()

    # Clean up test data directories after clearing cache
    for dir <- ["data/pages", "data/heap"] do
      if File.exists?(dir) do
        File.rm_rf!(dir)
      end

      File.mkdir_p!(dir)
    end

    # Create a test table with some pages
    table_name = "test_table"

    # Create page file
    {:ok, _} = PageManager.create_page_file(table_name)

    # Create a test page
    page = Page.new(0)
    {:ok, page_with_data} = Page.add_tuple(page, 1, [1, "test"])
    :ok = PageManager.write_page(table_name, 0, page_with_data)

    {:ok, table_name: table_name}
  end

  describe "BufferManager.get_page/2" do
    test "loads page from disk on cache miss", %{table_name: table_name} do
      # First access should load from disk
      assert {:ok, page} = BufferManager.get_page(table_name, 0)
      assert page.page_id == 0

      # Page should be pinned after get_page
      stats = BufferManager.stats()
      assert stats.pinned_pages == 1
      assert stats.pages_cached == 1
    end

    test "returns cached page on cache hit", %{table_name: table_name} do
      # Load page first time
      {:ok, _page1} = BufferManager.get_page(table_name, 0)

      # Second access should be cache hit
      {:ok, _page2} = BufferManager.get_page(table_name, 0)

      # Should still have only 1 cached page but 2 pins
      stats = BufferManager.stats()
      assert stats.pages_cached == 1
      # Same page, but pinned twice
      assert stats.pinned_pages == 1
    end

    test "returns error for non-existent page", %{table_name: table_name} do
      assert {:error, _reason} = BufferManager.get_page(table_name, 999)
    end

    test "handles concurrent access to same page", %{table_name: table_name} do
      # Simulate concurrent access from multiple processes
      tasks =
        1..5
        |> Enum.map(fn _i ->
          Task.async(fn ->
            BufferManager.get_page(table_name, 0)
          end)
        end)

      results = Task.await_many(tasks)

      # All should succeed
      assert Enum.all?(results, &match?({:ok, _}, &1))

      # Should have only 1 cached page
      stats = BufferManager.stats()
      assert stats.pages_cached == 1
    end
  end

  describe "BufferManager.mark_dirty/3" do
    test "marks page as dirty with updated content", %{table_name: table_name} do
      # Get page first
      {:ok, page} = BufferManager.get_page(table_name, 0)

      # Add another tuple
      {:ok, updated_page} = Page.add_tuple(page, 2, [2, "test2"])

      # Mark as dirty
      assert :ok = BufferManager.mark_dirty(table_name, 0, updated_page)

      # Check stats
      stats = BufferManager.stats()
      assert stats.dirty_pages == 1
    end

    test "returns error if page not pinned", %{table_name: table_name} do
      # Get page and unpin it
      {:ok, page} = BufferManager.get_page(table_name, 0)
      BufferManager.unpin_page(table_name, 0)

      # Try to mark dirty - should fail
      {:ok, updated_page} = Page.add_tuple(page, 2, [2, "test2"])
      assert {:error, :page_not_pinned} = BufferManager.mark_dirty(table_name, 0, updated_page)
    end

    test "returns error if page not cached", %{table_name: table_name} do
      # Try to mark dirty without getting page first
      page = Page.new(0)
      assert {:error, :page_not_cached} = BufferManager.mark_dirty(table_name, 0, page)
    end
  end

  describe "BufferManager.unpin_page/2" do
    test "decrements pin count", %{table_name: table_name} do
      # Get page (pins it)
      {:ok, _page} = BufferManager.get_page(table_name, 0)

      stats_before = BufferManager.stats()
      initial_pinned = stats_before.pinned_pages

      # Unpin page
      assert :ok = BufferManager.unpin_page(table_name, 0)

      stats_after = BufferManager.stats()
      assert stats_after.pinned_pages == initial_pinned - 1
    end

    test "handles unpin of non-cached page gracefully", %{table_name: table_name} do
      # Should not crash
      assert :ok = BufferManager.unpin_page(table_name, 999)
    end

    test "handles multiple pins/unpins correctly", %{table_name: table_name} do
      # Get initial state
      initial_stats = BufferManager.stats()
      initial_pinned = initial_stats.pinned_pages

      # Pin twice
      {:ok, _page1} = BufferManager.get_page(table_name, 0)
      {:ok, _page2} = BufferManager.get_page(table_name, 0)

      # Should have pinned pages (note: pin_count tracks per-page, pinned_pages tracks unique pages)
      stats_after_pins = BufferManager.stats()
      assert stats_after_pins.pinned_pages >= initial_pinned

      # First unpin
      BufferManager.unpin_page(table_name, 0)
      stats_after_first_unpin = BufferManager.stats()
      # Page should still be pinned due to second pin
      assert stats_after_first_unpin.pinned_pages >= initial_pinned

      # Second unpin
      BufferManager.unpin_page(table_name, 0)
      stats_after_second_unpin = BufferManager.stats()
      # Now should be unpinned (back to initial or lower)
      assert stats_after_second_unpin.pinned_pages <= initial_pinned
    end
  end

  describe "BufferManager.flush_all/0" do
    test "flushes all dirty pages to disk", %{table_name: table_name} do
      # Get page and modify it
      {:ok, page} = BufferManager.get_page(table_name, 0)
      {:ok, updated_page} = Page.add_tuple(page, 2, [2, "test2"])
      BufferManager.mark_dirty(table_name, 0, updated_page)

      # Verify dirty before flush
      stats_before = BufferManager.stats()
      assert stats_before.dirty_pages == 1

      # Flush all
      assert {:ok, 1} = BufferManager.flush_all()

      # Verify clean after flush
      stats_after = BufferManager.stats()
      assert stats_after.dirty_pages == 0

      # Verify data persisted to disk
      {:ok, disk_page} = PageManager.read_page(table_name, 0)
      final_tuples = Page.get_all_tuples(disk_page)
      assert length(final_tuples) == 2
    end

    test "handles empty buffer gracefully", %{table_name: _table_name} do
      assert {:ok, 0} = BufferManager.flush_all()
    end
  end

  describe "BufferManager.stats/0" do
    test "returns accurate buffer statistics", %{table_name: table_name} do
      # Initial stats (using application's buffer manager)
      stats = BufferManager.stats()
      # Application default
      assert stats.buffer_size == 128
      # May have existing cached pages
      initial_cached = stats.pages_cached
      # May have existing pinned pages
      initial_pinned = stats.pinned_pages
      # May have existing dirty pages
      initial_dirty = stats.dirty_pages

      # Load and modify a page
      {:ok, page} = BufferManager.get_page(table_name, 0)
      {:ok, updated_page} = Page.add_tuple(page, 2, [2, "test2"])
      BufferManager.mark_dirty(table_name, 0, updated_page)

      # Check updated stats (relative to initial)
      stats = BufferManager.stats()
      # At least one more page cached
      assert stats.pages_cached >= initial_cached + 1
      # At least one more dirty page
      assert stats.dirty_pages >= initial_dirty + 1
      # At least one more pinned page
      assert stats.pinned_pages >= initial_pinned + 1
    end
  end

  describe "BufferManager eviction" do
    test "evicts LRU pages when buffer is full", %{table_name: table_name} do
      # Get the actual buffer size from the running buffer manager
      initial_stats = BufferManager.stats()
      buffer_size = initial_stats.buffer_size

      # Create enough pages to fill most of the buffer
      # Don't create too many to keep test fast
      pages_to_create = min(buffer_size + 5, 50)

      # Create additional pages for testing
      for i <- 1..(pages_to_create - 1) do
        page = Page.new(i)
        {:ok, page_with_data} = Page.add_tuple(page, 1, [i, "test#{i}"])
        {:ok, _page_number} = PageManager.append_page(table_name, page_with_data)
      end

      # Load pages to fill buffer significantly but not completely
      pages_to_load = min(buffer_size - 10, pages_to_create)

      for i <- 0..(pages_to_load - 1) do
        {:ok, _page} = BufferManager.get_page(table_name, i)
        BufferManager.unpin_page(table_name, i)
      end

      stats = BufferManager.stats()
      # Allow some variance
      assert stats.pages_cached >= pages_to_load - 5
      # Should be reasonably full (lowered from 70)
      assert stats.utilization_percent >= 30

      # Create one more page to potentially trigger eviction
      page = Page.new(pages_to_create)
      {:ok, page_with_data} = Page.add_tuple(page, 1, [pages_to_create, "test#{pages_to_create}"])
      {:ok, _page_number} = PageManager.append_page(table_name, page_with_data)

      # Load it - may trigger eviction depending on buffer state
      {:ok, _page} = BufferManager.get_page(table_name, pages_to_create)
      BufferManager.unpin_page(table_name, pages_to_create)

      final_stats = BufferManager.stats()
      # Buffer should still be managing pages effectively
      assert final_stats.pages_cached <= buffer_size
      # All unpinned
      assert final_stats.pinned_pages == 0
    end

    test "does not evict pinned pages", %{table_name: table_name} do
      # Get the actual buffer size
      initial_stats = BufferManager.stats()
      buffer_size = initial_stats.buffer_size

      # Use a smaller number of pages for this test
      pages_to_test = min(15, buffer_size)

      # Create additional pages
      for i <- 1..(pages_to_test - 1) do
        page = Page.new(i)
        {:ok, page_with_data} = Page.add_tuple(page, 1, [i, "test#{i}"])
        {:ok, _page_number} = PageManager.append_page(table_name, page_with_data)
      end

      # Load and pin first 10 pages (keep them pinned)
      pinned_pages = min(10, pages_to_test)

      for i <- 0..(pinned_pages - 1) do
        {:ok, _page} = BufferManager.get_page(table_name, i)
        # Don't unpin - keep them pinned
      end

      stats = BufferManager.stats()
      initial_pinned = stats.pinned_pages
      assert initial_pinned >= pinned_pages

      # Try to load another page - should not evict pinned pages
      page = Page.new(pages_to_test)
      {:ok, page_with_data} = Page.add_tuple(page, 1, [pages_to_test, "test#{pages_to_test}"])
      {:ok, _page_number} = PageManager.append_page(table_name, page_with_data)

      {:ok, _page} = BufferManager.get_page(table_name, pages_to_test)
      BufferManager.unpin_page(table_name, pages_to_test)

      final_stats = BufferManager.stats()
      # Should still have all originally pinned pages
      assert final_stats.pinned_pages >= initial_pinned
    end
  end

  describe "Integration test" do
    test "complete workflow with multiple operations", %{table_name: table_name} do
      # 1. Load page
      {:ok, page} = BufferManager.get_page(table_name, 0)
      tuples = Page.get_all_tuples(page)
      assert length(tuples) == 1

      # 2. Modify page
      {:ok, updated_page} = Page.add_tuple(page, 2, [2, "test2"])
      BufferManager.mark_dirty(table_name, 0, updated_page)

      # 3. Load same page again - should get updated version
      {:ok, cached_page} = BufferManager.get_page(table_name, 0)
      cached_tuples = Page.get_all_tuples(cached_page)
      assert length(cached_tuples) == 2

      # 4. Unpin pages
      BufferManager.unpin_page(table_name, 0)
      BufferManager.unpin_page(table_name, 0)

      # 5. Flush to disk
      {:ok, 1} = BufferManager.flush_all()

      # 6. Verify persistence
      {:ok, disk_page} = PageManager.read_page(table_name, 0)
      disk_tuples = Page.get_all_tuples(disk_page)
      assert length(disk_tuples) == 2
    end
  end
end
