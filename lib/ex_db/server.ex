defmodule ExDb.Server do
  use GenServer

  alias ExDb.Wire.Protocol
  alias ExDb.Wire.ErrorMessage
  alias ExDb.Wire.Transport
  alias ExDb.Errors

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

    # Initialize storage state for page-based heap storage
    storage_state = ExDb.Storage.Heap.new("dummy_table")

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
    remote_ip = :inet_parse.ntoa(address) |> to_string()

    # Generate simple connection ID for correlation
    connection_id = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)

    # Set Logger metadata for this process - idiomatic way to add context
    Logger.metadata(
      connection_id: connection_id,
      remote_ip: remote_ip,
      remote_port: port
    )

    Logger.info("Connection established",
      remote_ip: remote_ip,
      remote_port: port
    )

    case Protocol.handle_startup(socket) do
      {:ok, params} ->
        Logger.info("Authentication successful",
          params: inspect(params),
          protocol_version: params["protocol_version"] || "unknown"
        )

        # Keep connection open for queries
        handle_queries(socket, connection_id, storage_state)

      {:error, :invalid_protocol, protocol_version} ->
        Logger.warning("Authentication failed",
          reason: "invalid_protocol",
          protocol_version: inspect(protocol_version)
        )

        # Error response was already sent, just close the connection
        :gen_tcp.close(socket)
        Logger.info("Connection closed", reason: "protocol_error")

      {:error, :malformed} ->
        Logger.warning("Authentication failed", reason: "malformed_packet")
        # Malformed packet: just close the connection (no error message sent)
        :gen_tcp.close(socket)
        Logger.info("Connection closed", reason: "malformed_packet")
    end
  end

  defp handle_queries(socket, connection_id, storage_state) do
    case Protocol.handle_query(socket, storage_state) do
      {:ok, new_storage_state} ->
        # Query handled successfully, continue listening for more queries
        handle_queries(socket, connection_id, new_storage_state)

      {:error, :closed} ->
        Logger.info("Connection closed", reason: "client_disconnect")
        # Client closed connection gracefully
        :ok

      {:error, :malformed} ->
        # Malformed query: log warning but keep connection open for now
        # In a production system, we might want to close after multiple consecutive errors
        Logger.warning("Malformed query received", action: "continuing")

        # Send standardized error response and continue
        exception = Errors.ProtocolViolationError.exception("malformed query packet")
        error_msg = ErrorMessage.from_exception(exception)
        Transport.send_error_message(socket, error_msg)
        handle_queries(socket, connection_id, storage_state)

      {:error, reason} ->
        Logger.warning("Query processing error",
          error: inspect(reason),
          action: "continuing"
        )

        # For other errors, send standardized error response and continue
        exception = Errors.ConnectionFailureError.exception(inspect(reason))
        error_msg = ErrorMessage.from_exception(exception)
        Transport.send_error_message(socket, error_msg)
        handle_queries(socket, connection_id, storage_state)
    end
  end
end
