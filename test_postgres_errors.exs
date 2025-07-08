#!/usr/bin/env elixir

# Test script to see real Postgres error messages
# Usage: elixir test_postgres_errors.exs

defmodule PostgresErrorTest do
  def test_invalid_protocol do
    port = 5433  # Change this to your real Postgres port
    {:ok, socket} = :gen_tcp.connect('localhost', port, [:binary, active: false])

    # Send invalid protocol version
    invalid_protocol = <<0, 0, 0, 8, 255, 255, 255, 255>>
    :ok = :gen_tcp.send(socket, invalid_protocol)

    # Read response
    {:ok, response} = :gen_tcp.recv(socket, 0, 1000)

    IO.puts("Real Postgres error response:")
    IO.inspect(response, base: :hex)
    IO.puts("As string: #{inspect(response)}")

    :gen_tcp.close(socket)
  end

  def test_insufficient_data do
    port = 5433
    {:ok, socket} = :gen_tcp.connect('localhost', port, [:binary, active: false])

    # Send just length, no data
    insufficient = <<0, 0, 0, 4>>
    :ok = :gen_tcp.send(socket, insufficient)

    {:ok, response} = :gen_tcp.recv(socket, 0, 1000)

    IO.puts("Real Postgres insufficient data error:")
    IO.inspect(response, base: :hex)
    IO.puts("As string: #{inspect(response)}")

    :gen_tcp.close(socket)
  end
end

# Run tests
PostgresErrorTest.test_invalid_protocol()
PostgresErrorTest.test_insufficient_data()
