defmodule StorageBenchmark do
  @moduledoc """
  Comprehensive benchmarks for ExDb storage performance.

  This benchmark suite tests different database operations to establish
  performance baselines and measure improvements from optimizations
  like buffer management.

  Usage:
    mix run benchmark/storage_benchmark.exs
  """

  alias ExDb.{SQL.Parser, Executor, TableStorage.Heap}
  require Logger

  @doc """
  Runs the complete benchmark suite
  """
  def run do
    IO.puts("🚀 ExDb Storage Performance Benchmark")
    IO.puts("=====================================\n")

    # Disable debug logging for cleaner output
    original_level = Logger.level()
    Logger.configure(level: :info)

    # Cleanup before starting
    cleanup_benchmark_files()

    # Run benchmarks with different data sizes
    small_dataset_benchmarks()
    medium_dataset_benchmarks()

    # Restore original log level
    Logger.configure(level: original_level)

    IO.puts("\n✅ Benchmark complete! Results show current performance baseline.")
    IO.puts("💡 After implementing buffer manager, run again to see improvements!")
  end

  defp small_dataset_benchmarks do
    IO.puts("📊 Small Dataset Benchmarks (100 rows)")
    IO.puts("------------------------------------")

    Benchee.run(
      %{
        "sequential_inserts" => fn -> benchmark_sequential_inserts(100) end,
        "random_selects" => fn -> benchmark_random_selects(100, 50) end,
        "full_table_scan" => fn -> benchmark_full_table_scan(100) end,
        "hot_data_access" => fn -> benchmark_hot_data_access(100, 10) end
      },
      time: 3,
      memory_time: 1,
      warmup: 1,
      formatters: [
        Benchee.Formatters.Console
      ],
      print: [
        benchmarking: true,
        configuration: false,
        fast_warning: false
      ]
    )
  end

  defp medium_dataset_benchmarks do
    IO.puts("\n📊 Medium Dataset Benchmarks (1000 rows)")
    IO.puts("--------------------------------------")

    Benchee.run(
      %{
        "sequential_inserts_1k" => fn -> benchmark_sequential_inserts(1000) end,
        "random_selects_1k" => fn -> benchmark_random_selects(1000, 100) end,
        "full_table_scan_1k" => fn -> benchmark_full_table_scan(1000) end,
        "mixed_workload_1k" => fn -> benchmark_mixed_workload(1000, 200) end
      },
      time: 5,
      memory_time: 2,
      warmup: 2,
      formatters: [
        Benchee.Formatters.Console
      ],
      print: [
        benchmarking: true,
        configuration: false,
        fast_warning: false
      ]
    )
  end

  # Benchmark Functions
  # ===================

  defp benchmark_sequential_inserts(count) do
    table_name = "bench_inserts_#{:erlang.unique_integer([:positive])}"
    state = setup_table(table_name)
    adapter = {Heap, state}

    1..count
    |> Enum.reduce(adapter, fn i, acc_adapter ->
      {:ok, ast} =
        Parser.parse(
          "INSERT INTO #{table_name} VALUES (#{i}, 'User#{i}', 'user#{i}@example.com')"
        )

      {:ok, new_adapter} = Executor.execute(ast, acc_adapter)
      new_adapter
    end)

    :ok
  end

  defp benchmark_random_selects(table_size, select_count) do
    table_name = "bench_selects_#{:erlang.unique_integer([:positive])}"
    adapter = setup_table_with_data(table_name, table_size)

    1..select_count
    |> Enum.each(fn _i ->
      random_id = :rand.uniform(table_size)
      {:ok, ast} = Parser.parse("SELECT * FROM #{table_name} WHERE id = #{random_id}")
      {:ok, _result, _columns, _adapter} = Executor.execute(ast, adapter)
    end)

    :ok
  end

  defp benchmark_full_table_scan(table_size) do
    table_name = "bench_scan_#{:erlang.unique_integer([:positive])}"
    adapter = setup_table_with_data(table_name, table_size)

    {:ok, ast} = Parser.parse("SELECT * FROM #{table_name}")
    {:ok, _result, _columns, _adapter} = Executor.execute(ast, adapter)

    :ok
  end

  defp benchmark_hot_data_access(table_size, hot_data_percent) do
    table_name = "bench_hot_#{:erlang.unique_integer([:positive])}"
    adapter = setup_table_with_data(table_name, table_size)

    # Access the first 10% of rows repeatedly (hot data)
    hot_row_count = max(1, div(table_size * hot_data_percent, 100))

    # 50 accesses to hot data
    1..50
    |> Enum.each(fn _i ->
      hot_id = :rand.uniform(hot_row_count)
      {:ok, ast} = Parser.parse("SELECT * FROM #{table_name} WHERE id = #{hot_id}")
      {:ok, _result, _columns, _adapter} = Executor.execute(ast, adapter)
    end)

    :ok
  end

  defp benchmark_mixed_workload(table_size, operation_count) do
    table_name = "bench_mixed_#{:erlang.unique_integer([:positive])}"
    adapter = setup_table_with_data(table_name, table_size)

    1..operation_count
    |> Enum.reduce(adapter, fn i, acc_adapter ->
      if rem(i, 10) < 7 do
        # 70% reads
        random_id = :rand.uniform(table_size)
        {:ok, ast} = Parser.parse("SELECT * FROM #{table_name} WHERE id = #{random_id}")
        {:ok, _result, _columns, new_adapter} = Executor.execute(ast, acc_adapter)
        new_adapter
      else
        # 30% writes (additional inserts)
        new_id = table_size + i

        {:ok, ast} =
          Parser.parse(
            "INSERT INTO #{table_name} VALUES (#{new_id}, 'NewUser#{i}', 'new#{i}@example.com')"
          )

        {:ok, new_adapter} = Executor.execute(ast, acc_adapter)
        new_adapter
      end
    end)

    :ok
  end

  # Helper Functions
  # ================

  defp setup_table(table_name) do
    state = Heap.new(table_name)
    {:ok, state} = Heap.create_table(state, table_name, [])
    state
  end

  defp setup_table_with_data(table_name, row_count) do
    state = setup_table(table_name)
    adapter = {Heap, state}

    final_adapter =
      1..row_count
      |> Enum.reduce(adapter, fn i, acc_adapter ->
        {:ok, ast} =
          Parser.parse(
            "INSERT INTO #{table_name} VALUES (#{i}, 'User#{i}', 'user#{i}@example.com')"
          )

        {:ok, new_adapter} = Executor.execute(ast, acc_adapter)
        new_adapter
      end)

    final_adapter
  end

  defp cleanup_benchmark_files do
    # Clean up any leftover benchmark data
    if File.exists?("data/pages") do
      "data/pages"
      |> File.ls!()
      |> Enum.filter(&String.contains?(&1, "bench_"))
      |> Enum.each(fn file ->
        File.rm(Path.join("data/pages", file))
      end)
    end
  end
end

# Run the benchmark if this file is executed directly
if __ENV__.file == Path.absname(__ENV__.file) do
  StorageBenchmark.run()
end
