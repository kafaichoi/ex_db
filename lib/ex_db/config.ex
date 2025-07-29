defmodule ExDb.Config do
  @moduledoc """
  Application configuration for ExDb.

  Provides runtime configuration access with sensible defaults.
  In idiomatic Elixir, prefer calling Application.get_env/3 directly
  where configuration is used, rather than wrapping in functions.
  """

  # Default values for runtime configuration
  @default_port 5432
  @default_host "0.0.0.0"
  @default_max_connections 100
  @default_query_timeout 30_000
  @default_connection_timeout 10_000
  @default_test_timeout 1_000

  # Compile-time constant
  @protocol_version 0x00030000

  @doc """
  Get runtime configuration values with defaults.

  In idiomatic Elixir, prefer calling Application.get_env/3 directly.
  These functions are provided for convenience where multiple modules
  need the same configuration with the same defaults.
  """
  def port, do: Application.get_env(:ex_db, :port, @default_port)
  def host, do: Application.get_env(:ex_db, :host, @default_host)
  def max_connections, do: Application.get_env(:ex_db, :max_connections, @default_max_connections)
  def query_timeout, do: Application.get_env(:ex_db, :query_timeout, @default_query_timeout)

  def connection_timeout,
    do: Application.get_env(:ex_db, :connection_timeout, @default_connection_timeout)

  def test_timeout, do: Application.get_env(:ex_db, :test_timeout, @default_test_timeout)

  @doc """
  Get the supported PostgreSQL wire protocol version.
  """
  def protocol_version, do: @protocol_version

  @doc """
  Check if a protocol version is supported.
  """
  def supported_protocol_version?(version), do: version == @protocol_version
end
