defmodule ExDb.Server do
  use GenServer

  require Logger

  # Postgres AuthenticationOk message
  @auth_ok <<"R", 0, 0, 0, 8, 0, 0, 0, 0>>
  # ReadyForQuery (Idle state)
  @ready_for_query <<"Z", 0, 0, 0, 5, ?I>>
  # BackendKeyData
  @backend_key_data <<"K", 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2>>
  @parameter_status_server_version <<"S", 0, 0, 0, 19, "server_version", 0, "15.1", 0>>
  @parameter_status_server_encoding <<"S", 0, 0, 0, 19, "server_encoding", 0, "UTF8", 0>>
  @parameter_status_client_encoding <<"S", 0, 0, 0, 19, "client_encoding", 0, "UTF8", 0>>
  @parameter_status_application_name <<"S", 0, 0, 0, 19, "application_name", 0, "ex_db", 0>>
  @parameter_status_date_style <<"S", 0, 0, 0, 19, "DateStyle", 0, "ISO, MDY", 0>>
  @parameter_status_timezone <<"S", 0, 0, 0, 19, "TimeZone", 0, "UTC", 0>>
  @parameter_status_integer_datetimes <<"S", 0, 0, 0, 19, "integer_datetimes", 0, "on", 0>>
  @parameter_status_standard_conforming_strings <<"S", 0, 0, 0, 19, "standard_conforming_strings",
                                                  0, "on", 0>>
  @parameter_status_interval_style <<"S", 0, 0, 0, 19, "IntervalStyle", 0, "postgres", 0>>
  @parameter_status_is_superuser <<"S", 0, 0, 0, 19, "is_superuser", 0, "off", 0>>
  @parameter_status_session_authorization <<"S", 0, 0, 0, 19, "session_authorization", 0,
                                            "testuser", 0>>
  @parameter_status_in_hot_standby <<"S", 0, 0, 0, 19, "in_hot_standby", 0, "off", 0>>

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    port = Application.get_env(:ex_db, :port, 5432)

    {:ok, listen_socket} =
      :gen_tcp.listen(port, [:binary, packet: :raw, active: false, reuseaddr: true])

    spawn_link(fn -> accept_loop(listen_socket) end)
    {:ok, %{listen_socket: listen_socket, port: port}}
  end

  defp accept_loop(listen_socket) do
    {:ok, socket} = :gen_tcp.accept(listen_socket)
    spawn_link(fn -> handle_client(socket) end)
    accept_loop(listen_socket)
  end

  defp handle_client(socket) do
    case read_startup_packet(socket) do
      {:ok, _params} ->
        send_handshake(socket)
        # Keep connection open for queries (we'll implement this next)
        handle_queries(socket)

      {:error, reason} ->
        send_error(socket, reason)
        :gen_tcp.close(socket)
    end
  end

  defp read_startup_packet(socket) do
    case :gen_tcp.recv(socket, 4, 1000) do
      {:ok, <<length::32>>} ->
        if length < 4 do
          {:error, "Invalid packet length"}
        else
          data_length = length - 4

          case :gen_tcp.recv(socket, data_length, 1000) do
            {:ok, data} ->
              parse_startup_data(data)

            {:error, reason} ->
              {:error, "Failed to read startup data: #{reason}"}
          end
        end

      {:error, reason} ->
        {:error, "Failed to read packet length: #{reason}"}
    end
  end

  defp parse_startup_data(data) do
    case data do
      <<protocol_version::32, rest::binary>> ->
        if protocol_version == 0x00030000 do
          parse_parameters(rest)
        else
          {:error, "Unsupported protocol version: #{inspect(protocol_version)}"}
        end

      _ ->
        {:error, "Invalid startup packet format"}
    end
  end

  defp parse_parameters(data) do
    # For now, just extract user and database if present
    params = extract_key_params(data)
    {:ok, params}
  end

  defp extract_key_params(data) do
    # Simple parameter extraction - look for "user" and "database"
    case String.split(data, <<0>>) do
      parts when length(parts) >= 2 ->
        params =
          Enum.chunk_every(parts, 2)
          |> Enum.map(fn [key, value] -> {key, value} end)
          |> Enum.filter(fn {key, _} -> key in ["user", "database"] end)
          |> Map.new()

        params

      _ ->
        %{}
    end
  end

  defp send_handshake(socket) do
    # Send complete handshake sequence
    :gen_tcp.send(socket, @auth_ok)
    :gen_tcp.send(socket, @parameter_status_server_version)
    :gen_tcp.send(socket, @parameter_status_server_encoding)
    :gen_tcp.send(socket, @parameter_status_client_encoding)
    :gen_tcp.send(socket, @parameter_status_application_name)
    :gen_tcp.send(socket, @parameter_status_date_style)
    :gen_tcp.send(socket, @parameter_status_timezone)
    :gen_tcp.send(socket, @parameter_status_integer_datetimes)
    :gen_tcp.send(socket, @parameter_status_standard_conforming_strings)
    :gen_tcp.send(socket, @parameter_status_interval_style)
    :gen_tcp.send(socket, @parameter_status_is_superuser)
    :gen_tcp.send(socket, @parameter_status_session_authorization)
    :gen_tcp.send(socket, @parameter_status_in_hot_standby)
    :gen_tcp.send(socket, @backend_key_data)
    :gen_tcp.send(socket, @ready_for_query)
  end

  defp send_error(socket, reason) do
    # Send ErrorResponse message
    error_msg = "ERROR: #{reason}"
    error_packet = <<"E", 0, 0, 0, byte_size(error_msg) + 5, error_msg::binary, 0>>
    :gen_tcp.send(socket, error_packet)
  end

  defp handle_queries(socket) do
    # For now, just close the connection
    # We'll implement query handling in the next step
    :gen_tcp.close(socket)
  end
end
