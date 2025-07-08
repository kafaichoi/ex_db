defmodule ExDb.Wire.Protocol do
  @moduledoc """
  High-level Postgres wire protocol operations.
  """

  alias ExDb.Wire.Messages
  alias ExDb.Wire.Parser

  @doc """
  Handle the complete startup handshake with a client.
  Returns {:ok, params} on success, {:error, reason} on failure.
  """
  def handle_startup(socket) do
    case Parser.read_packet(socket) do
      {:ok, data} ->
        case Parser.parse_startup_packet(data) do
          {:ok, params} ->
            send_handshake(socket)
            {:ok, params}

          {:error, :invalid_protocol, protocol_version} ->
            # Invalid protocol version: send error response
            send_error(socket, "unsupported frontend protocol: #{inspect(protocol_version)}")
            {:error, :invalid_protocol, protocol_version}

          {:error, _reason} ->
            # Other parsing errors: just return error (no error message sent)
            {:error, :malformed}
        end

      {:error, :invalid_length} ->
        # Malformed/insufficient data: just return error (no error message sent)
        {:error, :malformed}

      {:error, _reason} ->
        # Other read errors: just return error (no error message sent)
        {:error, :malformed}
    end
  end

  @doc """
  Send the complete handshake sequence to the client.
  """
  def send_handshake(socket) do
    Messages.handshake_sequence()
    |> Enum.each(&:gen_tcp.send(socket, &1))
  end

  @doc """
  Send an error response to the client.
  """
  def send_error(socket, message, severity \\ "FATAL", code \\ "0A000") do
    error_packet = Messages.error_response(severity, code, message)
    :gen_tcp.send(socket, error_packet)
  end

  @doc """
  Send a ready for query message.
  """
  def send_ready_for_query(socket, state \\ ?I) do
    ready_packet = Messages.ready_for_query(state)
    :gen_tcp.send(socket, ready_packet)
  end

  @doc """
  Send a parameter status message.
  """
  def send_parameter_status(socket, name, value) do
    param_packet = Messages.parameter_status(name, value)
    :gen_tcp.send(socket, param_packet)
  end

  @doc """
  Validate if the protocol version is supported.
  """
  def supported_protocol_version?(version) do
    version == 0x00030000
  end
end
