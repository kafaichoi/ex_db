defmodule ExDbTest do
  use ExUnit.Case
  doctest ExDb

  setup do
    # Get the configured port for testing
    # port = Application.get_env(:ex_db, :port)
    port = 28817
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
    assert {:error, :closed} = :gen_tcp.recv(socket, 1, 1000)
    :gen_tcp.close(socket)

    # Test 2: Invalid protocol version
    {:ok, socket2} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])
    # Invalid version
    invalid_protocol = <<0, 0, 0, 8, 255, 255, 255, 255>>
    :ok = :gen_tcp.send(socket2, invalid_protocol)
    {:ok, response2} = :gen_tcp.recv(socket2, 0, 1000)

    to_string(response2)
    |> IO.inspect()

    assert byte_size(response2) > 0
    :gen_tcp.close(socket2)

    # Test 3: Nonsense packet (random bytes)
    {:ok, socket3} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])
    nonsense = :crypto.strong_rand_bytes(20)
    :ok = :gen_tcp.send(socket3, nonsense)
    {:ok, response3} = :gen_tcp.recv(socket3, 0, 1000)
    to_string(response3)
    assert byte_size(response3) > 0
    :gen_tcp.close(socket3)
  end

  test "server handles empty connection gracefully", %{port: port} do
    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    # Don't send anything, just close
    :gen_tcp.close(socket)
    # Should not crash the server
  end
end
