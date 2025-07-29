defmodule ExDb.TableStorage.PageTest do
  use ExUnit.Case, async: true
  alias ExDb.TableStorage.Page

  describe "Page.new/1" do
    test "creates an empty page with correct initial state" do
      page = Page.new(42)

      assert page.page_id == 42
      assert page.tuple_count == 0
      # After header
      assert page.free_start == 24
      # At end of page
      assert page.free_end == 8192
      assert page.flags == 0
      assert page.checksum == 0
      assert page.line_pointers == []
      assert page.tuple_data == <<>>
    end
  end

  describe "Page.add_tuple/3" do
    test "adds a tuple to an empty page" do
      page = Page.new(1)

      {:ok, updated_page} = Page.add_tuple(page, 100, ["Alice", 25])

      assert updated_page.tuple_count == 1
      # 24 + 4 (line pointer)
      assert updated_page.free_start == 28
      assert updated_page.free_start < updated_page.free_end
      assert length(updated_page.line_pointers) == 1
      assert byte_size(updated_page.tuple_data) > 0
    end

    test "adds multiple tuples to a page" do
      page = Page.new(1)

      {:ok, page1} = Page.add_tuple(page, 1, ["Alice", 25])
      {:ok, page2} = Page.add_tuple(page1, 2, ["Bob", 30])
      {:ok, page3} = Page.add_tuple(page2, 3, ["Charlie", 35])

      assert page3.tuple_count == 3
      assert length(page3.line_pointers) == 3

      # Free space should be decreasing
      assert page3.free_end < page2.free_end
      assert page2.free_end < page1.free_end
      assert page1.free_end < page.free_end
    end

    test "returns error when page is full" do
      page = Page.new(1)

      # Add many large tuples to fill the page
      large_data = String.duplicate("x", 1000)

      result =
        1..10
        |> Enum.reduce_while({:ok, page}, fn i, {:ok, current_page} ->
          case Page.add_tuple(current_page, i, [large_data]) do
            {:ok, new_page} -> {:cont, {:ok, new_page}}
            {:error, :no_space} -> {:halt, {:error, :no_space}}
          end
        end)

      # Should eventually run out of space
      assert {:error, :no_space} = result
    end
  end

  describe "Page.get_all_tuples/1" do
    test "retrieves all tuples from a page" do
      page = Page.new(1)

      {:ok, page1} = Page.add_tuple(page, 100, ["Alice", 25])
      {:ok, page2} = Page.add_tuple(page1, 200, ["Bob", 30])

      tuples = Page.get_all_tuples(page2)

      assert length(tuples) == 2
      assert {100, ["Alice", 25]} in tuples
      assert {200, ["Bob", 30]} in tuples
    end

    test "returns empty list for empty page" do
      page = Page.new(1)
      tuples = Page.get_all_tuples(page)

      assert tuples == []
    end
  end

  describe "Page.serialize/1 and Page.deserialize/1" do
    test "serializes and deserializes an empty page correctly" do
      original_page = Page.new(42)

      binary = Page.serialize(original_page)
      # Exactly 8KB
      assert byte_size(binary) == 8192

      deserialized_page = Page.deserialize(binary)

      assert deserialized_page.page_id == original_page.page_id
      assert deserialized_page.tuple_count == original_page.tuple_count
      assert deserialized_page.free_start == original_page.free_start
      assert deserialized_page.free_end == original_page.free_end
      assert deserialized_page.line_pointers == original_page.line_pointers
    end

    test "serializes and deserializes a page with tuples correctly" do
      original_page = Page.new(99)

      {:ok, page1} = Page.add_tuple(original_page, 1, ["test", 123])
      {:ok, page2} = Page.add_tuple(page1, 2, ["data", 456])

      binary = Page.serialize(page2)
      assert byte_size(binary) == 8192

      deserialized_page = Page.deserialize(binary)

      assert deserialized_page.page_id == page2.page_id
      assert deserialized_page.tuple_count == page2.tuple_count
      assert deserialized_page.free_start == page2.free_start
      assert deserialized_page.free_end == page2.free_end
      assert length(deserialized_page.line_pointers) == 2

      # Verify tuples are preserved
      original_tuples = Page.get_all_tuples(page2)
      deserialized_tuples = Page.get_all_tuples(deserialized_page)

      assert length(original_tuples) == length(deserialized_tuples)
      assert Enum.sort(original_tuples) == Enum.sort(deserialized_tuples)
    end
  end

  describe "Page.has_space_for?/2" do
    test "correctly reports available space" do
      page = Page.new(1)

      # Should have space for small tuples
      small_tuple_size = 50
      assert Page.has_space_for?(page, small_tuple_size) == true

      # Should not have space for huge tuples
      huge_tuple_size = 9000
      assert Page.has_space_for?(page, huge_tuple_size) == false
    end

    test "space decreases as tuples are added" do
      page = Page.new(1)

      # Initially should have lots of space
      assert Page.has_space_for?(page, 1000) == true

      # Add some tuples
      {:ok, page1} = Page.add_tuple(page, 1, [String.duplicate("x", 500)])
      {:ok, page2} = Page.add_tuple(page1, 2, [String.duplicate("y", 500)])

      # Should have less space now
      available_before = page.free_end - page.free_start
      available_after = page2.free_end - page2.free_start

      assert available_after < available_before
    end
  end

  describe "Page.stats/1" do
    test "provides correct statistics" do
      page = Page.new(1)
      {:ok, updated_page} = Page.add_tuple(page, 1, ["test"])

      stats = Page.stats(updated_page)

      assert stats.page_id == 1
      assert stats.tuple_count == 1
      assert stats.free_space > 0
      assert stats.utilization > 0
      assert stats.utilization < 100
      assert is_integer(stats.checksum)
    end
  end
end
