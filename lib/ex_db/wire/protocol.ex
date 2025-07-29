defmodule ExDb.Wire.Protocol do
  @moduledoc """
  High-level Postgres wire protocol operations.
  """

  alias ExDb.Wire.Messages
  alias ExDb.Wire.Parser
  alias ExDb.Wire.ErrorMessage
  alias ExDb.Wire.Transport
  alias ExDb.SQL.Parser, as: SQLParser
  alias ExDb.Executor
  alias ExDb.Storage.Heap
  alias ExDb.Errors
  require Logger

  # Wire protocol message types
  @msg_query ?Q
  @msg_terminate ?X

  # Protocol structure constants
  @min_message_length 5
  @length_field_size 4

  # Transaction status indicators
  @txn_status_idle ?I

  # Protocol version
  @protocol_version 0x00030000

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
            # Invalid protocol version: send standardized error response
            exception =
              Errors.ProtocolViolationError.exception(
                "unsupported frontend protocol: #{inspect(protocol_version)}"
              )

            error_msg = ErrorMessage.from_exception(exception)
            Transport.send_error_message(socket, error_msg)
            {:error, :invalid_protocol, protocol_version}

          {:error, _reason} ->
            # Other parsing errors: just return error (no error message sent)
            {:error, :malformed}
        end

      {:error, :invalid_length} ->
        # Malformed/insufficient data: just return error (no error message sent)
        {:error, :malformed}
    end
  end

  @doc """
  Handle a query from the client.
  Returns {:ok, storage_state} on success, {:error, reason} on failure.
  """
  def handle_query(socket, storage_state) do
    case read_normal_message(socket) do
      {:ok, %{type: @msg_query, data: data}} ->
        # No need to log routine query messages - too verbose
        process_query(socket, data, storage_state)

      {:ok, %{type: @msg_terminate, data: _data}} ->
        # Terminate message: client wants to close connection
        Logger.debug("Terminate message received")
        {:error, :closed}

      {:error, :closed} ->
        Logger.debug("Socket closed during read")
        {:error, :closed}

      {:error, :timeout} ->
        # Timeout is normal during idle periods - don't log at all
        handle_query(socket, storage_state)

      {:error, reason} ->
        # Read error: just return error (no error message sent)
        Logger.warning("Query read failed", error: inspect(reason))
        {:error, :malformed}
    end
  end

  defp read_normal_message(socket) do
    # Configurable timeout for better interactive experience
    case :gen_tcp.recv(
           socket,
           @min_message_length,
           Application.get_env(:ex_db, :query_timeout, 30_000)
         ) do
      {:ok, <<type, length::32>>} ->
        if length < @min_message_length do
          {:ok, %{type: type, data: <<>>}}
        else
          case :gen_tcp.recv(
                 socket,
                 length - @length_field_size,
                 Application.get_env(:ex_db, :connection_timeout, 10_000)
               ) do
            {:ok, data} ->
              {:ok, %{type: type, data: String.trim_trailing(data, <<0>>)}}

            {:error, reason} ->
              Logger.warning("Query payload read failed", error: inspect(reason))
              {:error, :malformed}
          end
        end

      {:error, :closed} = err ->
        err

      {:error, :timeout} = err ->
        # Return timeout explicitly instead of treating as malformed
        err

      {:error, reason} ->
        Logger.warning("Query header read failed", error: inspect(reason))
        {:error, :malformed}
    end
  end

  @doc """
  Process a SQL query using the parser and executor.
  Returns {:ok, storage_state} on success.
  """
  def process_query(socket, query, storage_state) do
    query_trimmed = String.trim(query)

    # Add query to Logger metadata for this process
    Logger.metadata(query: String.slice(query_trimmed, 0, 100))

    Logger.info("Query received",
      query_type: extract_query_type(query_trimmed),
      query_length: String.length(query_trimmed)
    )

    # Create storage adapter tuple
    adapter = {Heap, storage_state}

    case SQLParser.parse(query_trimmed) do
      {:ok, ast} ->
        Logger.debug("SQL parsed successfully", ast_type: ast.__struct__)
        execute_sql(socket, ast, adapter)

      {:error, reason} ->
        Logger.warning("SQL parsing failed",
          error: inspect(reason),
          query: String.slice(query_trimmed, 0, 50)
        )

        # Convert parsing errors to proper exceptions
        exception = Errors.from_parser_error(reason, query_trimmed)
        error_msg = ErrorMessage.from_exception(exception)
        Transport.send_error_message(socket, error_msg)
        {:ok, storage_state}
    end
  end

  # Execute parsed SQL and format response
  defp execute_sql(socket, ast, adapter) do
    operation = get_operation_type(ast)
    table_name = get_table_name(ast)

    case Executor.execute(ast, adapter) do
      {:ok, result, columns, {_adapter_module, new_storage_state}} ->
        # SELECT statement - format result rows with column metadata
        Logger.info("Query completed",
          operation: operation,
          table: table_name,
          rows_returned: length(result)
        )

        Transport.send_select_response(socket, result, columns)
        {:ok, new_storage_state}

      {:ok, {_adapter_module, new_storage_state}} ->
        # INSERT or CREATE TABLE statement - send appropriate response
        Logger.info("Query completed",
          operation: operation,
          table: table_name,
          rows_affected: 1
        )

        case ast do
          %{__struct__: ExDb.SQL.AST.InsertStatement} ->
            Transport.send_insert_response(socket)

          %{__struct__: ExDb.SQL.AST.CreateTableStatement} ->
            Transport.send_create_table_response(socket)

          _ ->
            # fallback
            Transport.send_insert_response(socket)
        end

        {:ok, new_storage_state}

      {:error, reason} ->
        Logger.warning("Query execution failed",
          operation: operation,
          table: table_name,
          error: inspect(reason)
        )

        # Convert executor errors to proper exceptions
        exception = Errors.from_executor_error(reason)
        error_msg = ErrorMessage.from_exception(exception)
        Transport.send_error_message(socket, error_msg)
        {:ok, elem(adapter, 1)}
    end
  end

  # Helper functions for structured logging
  defp extract_query_type(query) do
    query
    |> String.trim()
    |> String.upcase()
    |> String.split(" ", parts: 2)
    |> hd()
  rescue
    _ -> "UNKNOWN"
  end

  defp get_operation_type(%ExDb.SQL.AST.SelectStatement{}), do: "SELECT"
  defp get_operation_type(%ExDb.SQL.AST.InsertStatement{}), do: "INSERT"
  defp get_operation_type(%ExDb.SQL.AST.CreateTableStatement{}), do: "CREATE_TABLE"
  defp get_operation_type(_), do: "UNKNOWN"

  defp get_table_name(%ExDb.SQL.AST.SelectStatement{from: %{name: name}}), do: name
  defp get_table_name(%ExDb.SQL.AST.InsertStatement{table: %{name: name}}), do: name
  defp get_table_name(%ExDb.SQL.AST.CreateTableStatement{table: %{name: name}}), do: name
  defp get_table_name(_), do: nil

  @doc """
  Send the complete handshake sequence to the client.
  """
  def send_handshake(socket) do
    Messages.handshake_sequence()
    |> Enum.each(&:gen_tcp.send(socket, &1))
  end

  @doc """
  Send an error response to the client.

  Deprecated: Use proper exceptions with ErrorMessage.from_exception/1 for new code.
  This function is kept for backwards compatibility.
  """
  def send_error(socket, message, severity \\ "FATAL", _code \\ "0A000") do
    # Create a legacy internal error exception and convert to wire format
    exception = Errors.InternalError.exception(message)
    error_msg = ErrorMessage.from_exception(exception)
    # Override severity for backwards compatibility
    error_msg = %{error_msg | severity: severity, severity_v: severity}
    Transport.send_error_message(socket, error_msg)
  end

  @doc """
  Send a ready for query message.

  Deprecated: Use ExDb.Wire.Transport.send_ready_for_query/2 for new code.
  """
  def send_ready_for_query(socket, state \\ @txn_status_idle) do
    Transport.send_ready_for_query(socket, state)
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
    version == @protocol_version
  end
end
