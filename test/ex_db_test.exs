defmodule ExDbTest do
  use ExUnit.Case
  doctest ExDb

  alias ExDb.Wire.ErrorMessage

  setup do
    # Get the configured port for testing
    port = Application.get_env(:ex_db, :port)
    {:ok, port: port}
  end

  test "server responds to minimal Postgres startup message", %{port: port} do
    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    # Minimal Postgres startup packet: protocol version 3.0, no params
    # Format: [length (4 bytes)] [protocol (4 bytes)] [params (null-terminated pairs)] [terminator (0)]
    # We'll send just the header and terminator for now
    # 3.0
    protocol_version = <<3::16, 0::16>>
    payload = protocol_version <> <<0>>
    packet_len = byte_size(payload) + 4
    startup_packet = <<packet_len::32, payload::binary>>

    :ok = :gen_tcp.send(socket, startup_packet)
    {:ok, response} = :gen_tcp.recv(socket, 0, 1000)

    # For now, just assert we got a response (we'll refine this as we implement the server)
    assert byte_size(response) > 0

    :gen_tcp.close(socket)
  end

  test "server handles proper Postgres startup packet", %{port: port} do
    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    # Build a realistic Postgres startup packet
    # 3.0
    protocol_version = <<3::16, 0::16>>

    # Common parameters that psql sends
    params = [
      {"user", "testuser"},
      {"database", "testdb"},
      {"application_name", "psql"},
      {"client_encoding", "UTF8"}
    ]

    # Build parameter string
    param_string =
      Enum.map_join(params, "", fn {key, value} ->
        key <> <<0>> <> value <> <<0>>
      end)

    # Build complete packet
    payload = protocol_version <> param_string <> <<0>>
    packet_len = byte_size(payload) + 4
    startup_packet = <<packet_len::32, payload::binary>>

    :ok = :gen_tcp.send(socket, startup_packet)

    # Read response - should get multiple messages
    {:ok, response} = :gen_tcp.recv(socket, 0, 1000)

    # For now, just assert we got a response
    # Later we'll parse and verify specific message types
    assert byte_size(response) > 0

    # The response should start with 'R' (AuthenticationOk)
    assert <<first_byte, _::binary>> = response
    assert first_byte == ?R

    :gen_tcp.close(socket)
  end

  test "server handles malformed startup packets gracefully", %{port: port} do
    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    # Test 1: Insufficient data (too short)
    # Just length, no data
    :ok = :gen_tcp.send(socket, <<0, 0, 0, 4>>)
    assert {:error, :closed} == :gen_tcp.recv(socket, 0, 1000)

    # Test 2: Invalid protocol version
    {:ok, socket2} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])
    # Invalid version
    invalid_protocol = <<0, 0, 0, 8, 255, 255, 255, 255>>
    :ok = :gen_tcp.send(socket2, invalid_protocol)
    {:ok, response2} = :gen_tcp.recv(socket2, 0, 1000)

    # Parse the error message and assert on its content
    error = ErrorMessage.parse(response2)
    assert error.severity == "FATAL"
    assert error.code == "0A000"
    assert error.message =~ "unsupported frontend protocol"
    assert ErrorMessage.fatal?(error)
    assert ErrorMessage.error?(error)

    :gen_tcp.close(socket2)
  end

  defp receive_all(socket, timeout, acc \\ "") do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, data} -> receive_all(socket, timeout, acc <> data)
      {:error, :closed} -> acc
      {:error, _} -> acc
    end
  end

  test "server responds to simple SELECT 1 query", %{port: port} do
    alias ExDb.Wire.ResponseParser

    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    # Startup handshake
    protocol_version = <<3::16, 0::16>>

    # Common parameters that psql sends
    params = [
      {"user", "postgres"}
      # {"database", "testdb"},
      # {"application_name", "psql"},
      # {"client_encoding", "UTF8"}
    ]

    # Build parameter string
    param_string =
      Enum.map_join(params, "", fn {key, value} ->
        key <> <<0>> <> value <> <<0>>
      end)

    # Build complete packet
    payload = protocol_version <> param_string <> <<0>>
    packet_len = byte_size(payload) + 4
    startup_packet = <<packet_len::32, payload::binary>>

    :ok = :gen_tcp.send(socket, startup_packet)
    # Read and discard handshake response
    _ = receive_all(socket, 1000)

    # Send a simple query: Q message (SELECT 1;)
    query = "SELECT 1;"

    query_packet =
      <<?Q, byte_size(query) + 5::32, query::binary, 0>>

    :ok = :gen_tcp.send(socket, query_packet)

    # Read the response (should be RowDescription, DataRow, CommandComplete, ReadyForQuery)
    response = receive_all(socket, 1000)

    # Parse the response into individual messages
    case ResponseParser.parse_response(response) do
      {:error, reason} ->
        flunk("Failed to parse response: #{inspect(reason)}")

      messages ->
        [row_desc, data_row, cmd_complete, ready] = messages

        # Check RowDescription
        assert %ExDb.Wire.Messages.RowDescription{field_count: 1, fields: [field]} = row_desc
        assert field.name == "?column?"
        # int4
        assert field.type_oid == 23

        # Check DataRow
        assert %ExDb.Wire.Messages.DataRow{field_count: 1, fields: ["1"]} = data_row

        # Check CommandComplete
        assert %ExDb.Wire.Messages.CommandComplete{tag: "SELECT 1"} = cmd_complete

        # Check ReadyForQuery
        assert ready == :ready_for_query
    end

    :gen_tcp.close(socket)
  end

  test "server supports CREATE TABLE with column definitions and INSERT", %{port: port} do
    alias ExDb.Wire.ResponseParser

    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    # Startup handshake
    protocol_version = <<3::16, 0::16>>

    # Common parameters that psql sends
    params = [
      {"user", "postgres"}
      # {"database", "testdb"},
      # {"application_name", "psql"},
      # {"client_encoding", "UTF8"}
    ]

    # Build parameter string
    param_string =
      Enum.map_join(params, "", fn {key, value} ->
        key <> <<0>> <> value <> <<0>>
      end)

    # Build complete packet
    payload = protocol_version <> param_string <> <<0>>
    packet_len = byte_size(payload) + 4
    startup_packet = <<packet_len::32, payload::binary>>

    :ok = :gen_tcp.send(socket, startup_packet)
    # Read and discard handshake response
    _ = receive_all(socket, 1000)

    # Send a CREATE TABLE query (should now succeed)
    query = "CREATE TABLE test (id INTEGER);"
    query_packet = <<?Q, byte_size(query) + 5::32, query::binary, 0>>

    :ok = :gen_tcp.send(socket, query_packet)

    # Read the response (should be CommandComplete, ReadyForQuery)
    response = receive_all(socket, 1000)

    # Parse the response into individual messages
    case ResponseParser.parse_response(response) do
      {:error, reason} ->
        flunk("Failed to parse response: #{inspect(reason)}")

      messages ->
        [cmd_complete, ready] = messages

        # Check CommandComplete for CREATE TABLE
        assert %ExDb.Wire.Messages.CommandComplete{tag: "CREATE TABLE"} = cmd_complete

        # Check ReadyForQuery - connection should still be ready
        assert ready == :ready_for_query
    end

    # Test that we can INSERT into the created table
    insert_query = "INSERT INTO test VALUES (1);"
    insert_packet = <<?Q, byte_size(insert_query) + 5::32, insert_query::binary, 0>>
    :ok = :gen_tcp.send(socket, insert_packet)

    # Should get successful response
    response2 = receive_all(socket, 1000)

    case ResponseParser.parse_response(response2) do
      {:error, reason} ->
        flunk("Failed to parse insert response: #{inspect(reason)}")

      messages ->
        [cmd_complete, ready] = messages
        assert %ExDb.Wire.Messages.CommandComplete{tag: "INSERT 0 1"} = cmd_complete
        assert ready == :ready_for_query
    end

    :gen_tcp.close(socket)
  end
end
