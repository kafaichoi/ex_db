defmodule ExDb.Wire.Parser do
  @moduledoc """
  Parser for Postgres wire protocol packets.
  """

  # Protocol structure constants
  @length_field_size 4
  @min_startup_packet_size 8

  # Wire protocol values
  @null_terminator 0

  # Compile-time constant for protocol version
  @protocol_version 0x00030000

  @doc """
  Parse a startup packet from the client.
  Returns {:ok, params} or {:error, reason}
  """
  def parse_startup_packet(data) do
    case data do
      <<protocol_version::32, rest::binary>> ->
        if protocol_version == @protocol_version do
          parse_parameters(rest)
        else
          {:error, :invalid_protocol, protocol_version}
        end

      _ ->
        {:error, :malformed}
    end
  end

  @doc """
  Extract key parameters from the startup packet data.
  """
  def parse_parameters(data) do
    # Split by null bytes and extract key-value pairs
    case String.split(data, <<@null_terminator>>) do
      parts when length(parts) >= 2 ->
        params =
          parts
          |> Enum.chunk_every(2)
          |> Enum.map(fn [key, value] -> {key, value} end)
          |> Enum.filter(fn {key, _} -> key in ["user", "database"] end)
          |> Map.new()

        {:ok, params}

      _ ->
        {:ok, %{}}
    end
  end

  @doc """
  Read a complete packet from the socket.
  Returns {:ok, data} or {:error, reason}
  """
  def read_packet(socket) do
    # Increased timeout for startup packet to handle slow clients
    case :gen_tcp.recv(
           socket,
           @length_field_size,
           Application.get_env(:ex_db, :connection_timeout, 10_000)
         ) do
      {:ok, <<length::32>>} ->
        if length < @min_startup_packet_size do
          # Minimum valid startup packet is 8 bytes (length + protocol)
          {:error, :invalid_length}
        else
          data_length = length - @length_field_size

          # Use same timeout for reading the packet data
          case :gen_tcp.recv(
                 socket,
                 data_length,
                 Application.get_env(:ex_db, :connection_timeout, 10_000)
               ) do
            {:ok, data} ->
              {:ok, data}

            {:error, :timeout} ->
              {:error, :timeout}

            {:error, _reason} ->
              {:error, :invalid_length}
          end
        end

      {:error, :timeout} ->
        {:error, :timeout}

      {:error, _reason} ->
        {:error, :invalid_length}
    end
  end

  @doc """
  Parse a query message from the client.
  Returns {:ok, query_string} or {:error, reason}
  """
  def parse_query_message(data) do
    case data do
      <<?Q, length::32, rest::binary>> when length >= 5 ->
        # Remove null terminator
        query = String.slice(rest, 0, byte_size(rest) - 1)
        {:ok, query}

      _ ->
        {:error, :invalid_query_message}
    end
  end

  @doc """
  Extract specific parameters from startup data.
  """
  def extract_parameter(data, key) do
    case parse_parameters(data) do
      {:ok, params} -> Map.get(params, key)
      _ -> nil
    end
  end
end
