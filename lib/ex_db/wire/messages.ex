defmodule ExDb.Wire.Messages do
  @moduledoc """
  Postgres wire protocol message constants and builders.
  """

  # Message type constants
  @auth_ok "R"
  @ready_for_query "Z"
  @backend_key_data "K"
  @parameter_status "S"
  @error_response "E"

  # Authentication messages
  def auth_ok do
    <<@auth_ok, 0, 0, 0, 8, 0, 0, 0, 0>>
  end

  # Ready for query messages
  def ready_for_query(state \\ ?I) do
    <<@ready_for_query, 0, 0, 0, 5, state>>
  end

  # Backend key data (for query cancellation)
  def backend_key_data(process_id \\ 1, secret_key \\ 2) do
    <<@backend_key_data, 0, 0, 0, 12, process_id::32, secret_key::32>>
  end

  # Parameter status messages
  def parameter_status(name, value) do
    data = name <> <<0>> <> value <> <<0>>
    length = byte_size(data) + 4
    <<@parameter_status, length::32, data::binary>>
  end

  # Error response message
  def error_response(severity \\ "FATAL", code \\ "0A000", message) do
    # Format: 'E' + length + 'S' + severity + '\0' + 'V' + severity + '\0' + 'C' + code + '\0' + 'M' + message + '\0' + '\0'
    error_data =
      "S" <>
        severity <>
        <<0>> <>
        "V" <> severity <> <<0>> <> "C" <> code <> <<0>> <> "M" <> message <> <<0>> <> <<0>>

    error_length = byte_size(error_data) + 4
    <<@error_response, error_length::32, error_data::binary>>
  end

  # Common parameter status messages
  def server_version, do: parameter_status("server_version", "15.1")
  def server_encoding, do: parameter_status("server_encoding", "UTF8")
  def client_encoding, do: parameter_status("client_encoding", "UTF8")
  def application_name, do: parameter_status("application_name", "ex_db")
  def date_style, do: parameter_status("DateStyle", "ISO, MDY")
  def timezone, do: parameter_status("TimeZone", "UTC")
  def integer_datetimes, do: parameter_status("integer_datetimes", "on")
  def standard_conforming_strings, do: parameter_status("standard_conforming_strings", "on")
  def interval_style, do: parameter_status("IntervalStyle", "postgres")
  def is_superuser, do: parameter_status("is_superuser", "off")
  def session_authorization, do: parameter_status("session_authorization", "testuser")
  def in_hot_standby, do: parameter_status("in_hot_standby", "off")

  # Complete handshake sequence
  def handshake_sequence do
    [
      auth_ok(),
      server_version(),
      server_encoding(),
      client_encoding(),
      application_name(),
      date_style(),
      timezone(),
      integer_datetimes(),
      standard_conforming_strings(),
      interval_style(),
      is_superuser(),
      session_authorization(),
      in_hot_standby(),
      backend_key_data(),
      ready_for_query()
    ]
  end
end
