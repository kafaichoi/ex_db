#!/usr/bin/env elixir

# Test logging functionality
defmodule LoggingTest do
  def test_logging do
    IO.puts("=== Testing ExDb Logging ===")

    # Test 1: Start server in background
    IO.puts("1. Starting server...")
    server_pid = spawn(fn ->
      System.cmd("mix", ["run", "--no-halt"], cd: "/home/enchoi/OSS/ex_db")
    end)

    # Wait for server to start
    Process.sleep(3000)

    # Test 2: Test successful query
    IO.puts("2. Testing successful query (SELECT 1)...")
    {output1, _} = System.cmd("psql", ["-h", "localhost", "-p", "5432", "-U", "postgres", "-c", "SELECT 1;"], stderr_to_stdout: true)
    IO.puts("Result: #{String.trim(output1)}")

    # Test 3: Test failing query
    IO.puts("3. Testing failing query (SELECT 2)...")
    {output2, _} = System.cmd("psql", ["-h", "localhost", "-p", "5432", "-U", "postgres", "-c", "SELECT 2;"], stderr_to_stdout: true)
    IO.puts("Result: #{String.trim(output2)}")

    # Test 4: Test another successful query
    IO.puts("4. Testing another successful query...")
    {output3, _} = System.cmd("psql", ["-h", "localhost", "-p", "5432", "-U", "postgres", "-c", "SELECT 1;"], stderr_to_stdout: true)
    IO.puts("Result: #{String.trim(output3)}")

    # Clean up
    Process.sleep(1000)
    System.cmd("pkill", ["-f", "mix run"])

    IO.puts("\n=== Logging Test Complete ===")
    IO.puts("Check the server logs above to verify:")
    IO.puts("✓ New connection logs")
    IO.puts("✓ Query processing logs")
    IO.puts("✓ Connection termination logs")
  end
end

# Run the test
LoggingTest.test_logging()
