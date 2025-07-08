defmodule ExDbTest do
  use ExUnit.Case
  doctest ExDb

  test "server responds to minimal Postgres startup message" do
    port = 5432
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
end
