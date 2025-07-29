defmodule ExDb.Server do
  use GenServer

  alias ExDb.Wire.Protocol

  require Logger

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    # Use Application.get_env directly - more idiomatic than wrapper function
    port = Application.get_env(:ex_db, :port, 5432)

    {:ok, listen_socket} =
      :gen_tcp.listen(port, [:binary, packet: :raw, active: false, reuseaddr: true])

    # Initialize storage state (dummy state since SharedInMemory uses GenServer)
    storage_state = :shared

    spawn_link(fn -> accept_loop(listen_socket, storage_state) end)
    {:ok, %{listen_socket: listen_socket, port: port, storage_state: storage_state}}
  end

  defp accept_loop(listen_socket, storage_state) do
    {:ok, socket} = :gen_tcp.accept(listen_socket)
    spawn_link(fn -> handle_client(socket, storage_state) end)
    accept_loop(listen_socket, storage_state)
  end

  defp handle_client(socket, storage_state) do
    {:ok, {address, port}} = :inet.peername(socket)
    client_info = "#{:inet_parse.ntoa(address)}:#{port}"
    Logger.info("New connection from #{client_info}")

    case Protocol.handle_startup(socket) do
      {:ok, params} ->
        Logger.info(
          "Connection #{client_info} authenticated successfully with params: #{inspect(params)}"
        )

        # Keep connection open for queries
        handle_queries(socket, client_info, storage_state)

      {:error, :invalid_protocol, protocol_version} ->
        Logger.warning(
          "Connection #{client_info} failed: invalid protocol version #{inspect(protocol_version)}"
        )

        # Error response was already sent, just close the connection
        :gen_tcp.close(socket)
        Logger.info("Connection #{client_info} terminated due to protocol error")

      {:error, :malformed} ->
        Logger.warning("Connection #{client_info} failed: malformed startup packet")
        # Malformed packet: just close the connection (no error message sent)
        :gen_tcp.close(socket)
        Logger.info("Connection #{client_info} terminated due to malformed packet")
    end
  end

  defp handle_queries(socket, client_info, storage_state) do
    case Protocol.handle_query(socket, storage_state) do
      {:ok, new_storage_state} ->
        # Query handled successfully, continue listening for more queries
        handle_queries(socket, client_info, new_storage_state)

      {:error, :closed} ->
        Logger.info("Connection #{client_info} closed by client")
        # Client closed connection gracefully
        :ok

      {:error, :malformed} ->
        # Malformed query: log warning but keep connection open for now
        # In a production system, we might want to close after multiple consecutive errors
        Logger.warning("Connection #{client_info} received malformed query, continuing...")

        # Send error response and continue
        Protocol.send_error(socket, "malformed query", "ERROR")
        Protocol.send_ready_for_query(socket)
        handle_queries(socket, client_info, storage_state)

      {:error, reason} ->
        Logger.warning("Connection #{client_info} encountered error: #{inspect(reason)}")
        # For other errors, send error response and continue
        Protocol.send_error(socket, "connection error: #{inspect(reason)}", "ERROR")
        Protocol.send_ready_for_query(socket)
        handle_queries(socket, client_info, storage_state)
    end
  end
end
