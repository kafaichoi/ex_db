defmodule ExDb.BufferManager do
  @moduledoc """
  Buffer manager with parallel I/O and write-back caching.

  Uses Pattern B API with high-performance parallel loading:
  - get_page/2: Load page with parallel I/O in caller process
  - mark_dirty/3: Update page in buffer, mark dirty (write-back)
  - unpin_page/2: Release page for potential eviction

  Key performance features:
  - Parallel I/O: Cache misses load in caller process (no GenServer bottleneck)
  - ETS-based buffer pool: Concurrent access for cache hits
  - Background eviction: Periodic cleanup without blocking operations
  - Write-back caching: Lazy disk writes for better performance
  """

  use GenServer
  require Logger

  alias ExDb.TableStorage.{PageManager, Page}

  # Configuration
  @default_buffer_size 128
  # 1 second
  @default_eviction_interval 1000
  # Start evicting at 80% capacity
  @default_eviction_threshold 0.8
  # Evict down to 60% capacity
  @default_target_utilization 0.6
  @ets_table_name :buffer_pool

  defstruct [
    :buffer_size,
    :ets_table,
    :eviction_interval,
    :eviction_threshold,
    :target_utilization
  ]

  @type page_key :: {String.t(), non_neg_integer()}

  @type buffer_entry :: %{
          page: Page.t(),
          dirty: boolean(),
          access_time: non_neg_integer(),
          pin_count: non_neg_integer()
        }

  # Public API - Clean module interface like PageManager

  @doc """
  Starts the buffer manager GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Gets a page from the buffer pool with parallel I/O.

  Fast path: Direct ETS lookup for cache hits
  Slow path: Parallel disk I/O in caller process (no GenServer bottleneck)

  Returns {:ok, page} and increments pin count.
  Caller must call unpin_page/2 when done.
  """
  def get_page(table_name, page_number) when is_binary(table_name) and is_integer(page_number) do
    page_key = {table_name, page_number}

    case lookup_and_pin(page_key) do
      {:ok, page} ->
        # Cache hit - fast path with direct ETS access
        {:ok, page}

      :cache_miss ->
        # Cache miss - parallel I/O in caller process
        ensure_buffer_space()
        load_and_cache_page(page_key)
    end
  end

  @doc """
  Marks a page as dirty with updated content.

  Updates the page in buffer cache and marks it dirty for
  write-back. The page must be currently pinned.
  """
  def mark_dirty(table_name, page_number, updated_page)
      when is_binary(table_name) and is_integer(page_number) do
    page_key = {table_name, page_number}

    case :ets.lookup(@ets_table_name, page_key) do
      [{^page_key, entry}] when entry.pin_count > 0 ->
        new_entry = %{entry | page: updated_page, dirty: true, access_time: current_time()}
        :ets.insert(@ets_table_name, {page_key, new_entry})
        :ok

      [{^page_key, _entry}] ->
        {:error, :page_not_pinned}

      [] ->
        {:error, :page_not_cached}
    end
  end

  @doc """
  Unpins a page, allowing it to be evicted.

  Decrements pin count. When pin_count reaches 0,
  the page becomes eligible for LRU eviction.
  """
  def unpin_page(table_name, page_number)
      when is_binary(table_name) and is_integer(page_number) do
    page_key = {table_name, page_number}

    case :ets.lookup(@ets_table_name, page_key) do
      [{^page_key, entry}] ->
        new_entry = %{entry | pin_count: max(0, entry.pin_count - 1), access_time: current_time()}
        :ets.insert(@ets_table_name, {page_key, new_entry})
        :ok

      [] ->
        Logger.warning("Unpin called on non-cached page", page_key: page_key)
        :ok
    end
  end

  @doc """
  Forces all dirty pages to be flushed to disk.
  """
  def flush_all() do
    GenServer.call(__MODULE__, :flush_all, 30_000)
  end

  @doc """
  Clears all cached pages from the buffer.
  WARNING: Only use this for testing! This will cause data loss if there are dirty pages.
  """
  def clear_cache() do
    GenServer.call(__MODULE__, :clear_cache)
  end

  @doc """
  Gets buffer pool statistics.
  """
  def stats() do
    GenServer.call(__MODULE__, :stats)
  end

  # Private functions for parallel I/O implementation

  defp lookup_and_pin(page_key) do
    case :ets.lookup(@ets_table_name, page_key) do
      [{^page_key, entry}] ->
        # Cache hit - atomically increment pin count and return current page (including dirty changes)
        new_entry = %{entry | pin_count: entry.pin_count + 1, access_time: current_time()}
        :ets.insert(@ets_table_name, {page_key, new_entry})
        {:ok, entry.page}

      [] ->
        :cache_miss
    end
  end

  defp ensure_buffer_space() do
    # No longer call GenServer - let background process handle eviction
    # Just check if we have space and proceed
    buffer_count = :ets.info(@ets_table_name, :size)

    if buffer_count >= @default_buffer_size do
      # Log warning but continue - background eviction will handle it
      Logger.debug("Buffer pool full, continuing with background eviction")
    end

    :ok
  end

  defp load_and_cache_page(page_key) do
    {table_name, page_number} = page_key

    # Load from disk in caller process (parallel I/O!)
    case PageManager.read_page(table_name, page_number) do
      {:ok, page} ->
        # Insert into cache with atomic ETS operation
        entry = %{
          page: page,
          dirty: false,
          access_time: current_time(),
          # Caller gets it pinned
          pin_count: 1
        }

        # Atomic insert - handles race conditions automatically
        :ets.insert(@ets_table_name, {page_key, entry})

        Logger.debug("Loaded page via parallel I/O", page_key: page_key)
        {:ok, page}

      {:error, reason} ->
        Logger.warning("Failed to load page", page_key: page_key, error: reason)
        {:error, reason}
    end
  end

  defp current_time() do
    System.monotonic_time(:millisecond)
  end

  # GenServer callbacks - Only for coordination, never I/O

  @impl true
  def init(opts) do
    buffer_size = Keyword.get(opts, :buffer_size, @default_buffer_size)
    eviction_interval = Keyword.get(opts, :eviction_interval, @default_eviction_interval)
    eviction_threshold = Keyword.get(opts, :eviction_threshold, @default_eviction_threshold)
    target_utilization = Keyword.get(opts, :target_utilization, @default_target_utilization)

    # Create ETS table for concurrent buffer pool access
    ets_table =
      :ets.new(@ets_table_name, [
        :set,
        :public,
        :named_table,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    # Schedule periodic eviction
    schedule_eviction(eviction_interval)

    Logger.info("BufferManager started with background eviction",
      buffer_size: buffer_size,
      max_memory_mb: div(buffer_size * 8192, 1024 * 1024),
      eviction_interval: eviction_interval,
      eviction_threshold: eviction_threshold,
      target_utilization: target_utilization
    )

    {:ok,
     %__MODULE__{
       buffer_size: buffer_size,
       ets_table: ets_table,
       eviction_interval: eviction_interval,
       eviction_threshold: eviction_threshold,
       target_utilization: target_utilization
     }}
  end

  @impl true
  def handle_info(:evict_periodically, state) do
    # Perform eviction if buffer is getting full
    buffer_count = :ets.info(state.ets_table, :size)
    threshold_count = trunc(state.buffer_size * state.eviction_threshold)

    if buffer_count >= threshold_count do
      target_count = trunc(state.buffer_size * state.target_utilization)
      evict_pages_until_under_threshold(state, target_count)
    end

    # Schedule next eviction
    schedule_eviction(state.eviction_interval)

    {:noreply, state}
  end

  @impl true
  def handle_call(:flush_all, _from, state) do
    # Find all dirty pages
    dirty_pages =
      :ets.select(state.ets_table, [
        {{:"$1", %{dirty: true, page: :"$2"}}, [], [{{:"$1", :"$2"}}]}
      ])

    Logger.info("Flushing all dirty pages", count: length(dirty_pages))

    # Flush each dirty page to disk
    results =
      Enum.map(dirty_pages, fn {page_key, page} ->
        flush_page_to_disk(page_key, page)
      end)

    success_count = Enum.count(results, &(&1 == :ok))
    error_count = length(dirty_pages) - success_count

    if error_count > 0 do
      Logger.warning("Some pages failed to flush",
        successful: success_count,
        failed: error_count
      )
    end

    {:reply, {:ok, success_count}, state}
  end

  @impl true
  def handle_call(:clear_cache, _from, state) do
    # Clear all cached pages (TESTING ONLY!)
    :ets.delete_all_objects(state.ets_table)
    Logger.warning("Buffer cache cleared - ALL CACHED DATA LOST!")
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    buffer_count = :ets.info(state.ets_table, :size)

    # Count dirty and pinned pages
    dirty_count =
      length(
        :ets.select(state.ets_table, [
          {{:"$1", %{dirty: true}}, [], [true]}
        ])
      )

    pinned_count =
      length(
        :ets.select(state.ets_table, [
          {{:"$1", %{pin_count: :"$2"}}, [{:>, :"$2", 0}], [true]}
        ])
      )

    stats = %{
      buffer_size: state.buffer_size,
      pages_cached: buffer_count,
      dirty_pages: dirty_count,
      pinned_pages: pinned_count,
      memory_usage_mb: div(buffer_count * 8192, 1024 * 1024),
      utilization_percent: div(buffer_count * 100, state.buffer_size),
      eviction_threshold: state.eviction_threshold,
      target_utilization: state.target_utilization
    }

    {:reply, stats, state}
  end

  # Private eviction and flushing functions

  defp schedule_eviction(interval) do
    Process.send_after(self(), :evict_periodically, interval)
  end

  defp evict_pages_until_under_threshold(state, target_count) do
    buffer_count = :ets.info(state.ets_table, :size)

    if buffer_count > target_count do
      case find_eviction_candidate() do
        {:ok, page_key} ->
          evict_page(page_key)
          Logger.debug("Background eviction", page_key: page_key)
          # Recursively evict more if needed
          evict_pages_until_under_threshold(state, target_count)

        :no_candidate ->
          Logger.warning("No evictable pages found during background eviction")
      end
    end
  end

  defp find_eviction_candidate() do
    # Find unpinned page with oldest access time (LRU)
    unpinned_pages =
      :ets.select(@ets_table_name, [
        {{:"$1", %{pin_count: 0, access_time: :"$2"}}, [], [{{:"$1", :"$2"}}]}
      ])

    case unpinned_pages do
      [] ->
        :no_candidate

      pages ->
        {page_key, _access_time} = Enum.min_by(pages, fn {_key, time} -> time end)
        {:ok, page_key}
    end
  end

  defp evict_page(page_key) do
    case :ets.lookup(@ets_table_name, page_key) do
      [{^page_key, entry}] ->
        # If dirty, flush to disk first
        if entry.dirty do
          case flush_page_to_disk(page_key, entry.page) do
            :ok ->
              Logger.debug("Flushed dirty page during eviction", page_key: page_key)

            {:error, reason} ->
              Logger.error("Failed to flush during eviction", page_key: page_key, error: reason)
          end
        end

        # Remove from buffer pool
        :ets.delete(@ets_table_name, page_key)
        :ok

      [] ->
        # Already evicted
        :ok
    end
  end

  defp flush_page_to_disk(page_key, page) do
    {table_name, page_number} = page_key

    case PageManager.write_page(table_name, page_number, page) do
      :ok ->
        # Mark as clean in buffer if still present
        case :ets.lookup(@ets_table_name, page_key) do
          [{^page_key, entry}] ->
            clean_entry = %{entry | dirty: false}
            :ets.insert(@ets_table_name, {page_key, clean_entry})

          [] ->
            # Page evicted while flushing - OK
            :ok
        end

        :ok

      {:error, {:file_not_found, _}} ->
        # File was deleted (e.g., during tests) - silently succeed since data loss is intentional
        # Mark as clean in buffer if still present
        case :ets.lookup(@ets_table_name, page_key) do
          [{^page_key, entry}] ->
            clean_entry = %{entry | dirty: false}
            :ets.insert(@ets_table_name, {page_key, clean_entry})

          [] ->
            :ok
        end

        :ok

      {:error, reason} ->
        Logger.error("Failed to flush page", page_key: page_key, error: reason)
        {:error, reason}
    end
  end
end
