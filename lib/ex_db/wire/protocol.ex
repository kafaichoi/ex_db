defmodule ExDb.Wire.Protocol do
  @moduledoc """
  High-level Postgres wire protocol operations.
  """

  alias ExDb.Wire.Messages
  alias ExDb.Wire.Parser
  alias ExDb.SQL.Parser, as: SQLParser
  alias ExDb.Executor
  alias ExDb.Storage.SharedInMemory
  require Logger

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
    end
  end

  @doc """
  Handle a query from the client.
  Returns {:ok, storage_state} on success, {:error, reason} on failure.
  """
  def handle_query(socket, storage_state) do
    case read_normal_message(socket) do
      {:ok, %{type: ?Q, data: data}} ->
        Logger.info("Received query message (Q), processing...")
        process_query(socket, data, storage_state)

      {:ok, %{type: ?X, data: _data}} ->
        # Terminate message: client wants to close connection
        Logger.info("Received terminate message (X), closing connection")
        {:error, :closed}

      {:error, :closed} ->
        Logger.info("Socket closed while reading query")
        {:error, :closed}

      {:error, reason} ->
        # Read error: just return error (no error message sent)
        Logger.warning("Failed to read query message: #{inspect(reason)}")
        {:error, :malformed}
    end
  end

  defp read_normal_message(socket) do
    case :gen_tcp.recv(socket, 5, 5000) do
      {:ok, <<type, length::32>>} ->
        if length < 5 do
          {:ok, %{type: type, data: <<>>}}
        else
          case :gen_tcp.recv(socket, length - 4, 5000) do
            {:ok, data} ->
              {:ok, %{type: type, data: String.trim_trailing(data, <<0>>)}}

            {:error, reason} ->
              Logger.warning("Failed to read query message: #{inspect(reason)}")
              {:error, :malformed}
          end
        end

      {:error, :closed} = err ->
        err

      {:error, :timeout} ->
        {:error, :malformed}
    end
  end

  @doc """
  Process a SQL query using the parser and executor.
  Returns {:ok, storage_state} on success.
  """
  def process_query(socket, query, storage_state) do
    query_trimmed = String.trim(query)
    Logger.info("Processing query: #{inspect(query_trimmed)}")

    # Create storage adapter tuple
    adapter = {SharedInMemory, storage_state}

    case SQLParser.parse(query_trimmed) do
      {:ok, ast} ->
        Logger.info("Successfully parsed SQL: #{inspect(ast)}")
        execute_sql(socket, ast, adapter)

      {:error, reason} ->
        Logger.warning("Failed to parse SQL: #{inspect(reason)}")
        # Convert parsing errors to more user-friendly messages
        error_msg =
          case reason do
            "Unexpected token: " <> _ -> "query not supported: #{query_trimmed}"
            _ -> "syntax error: #{inspect(reason)}"
          end

        send_error(socket, error_msg, "ERROR")
        send_ready_for_query(socket)
        {:ok, storage_state}
    end
  end

  # Execute parsed SQL and format response
  defp execute_sql(socket, ast, adapter) do
    case Executor.execute(ast, adapter) do
      {:ok, result, {_adapter_module, new_storage_state}} ->
        # SELECT statement - format result rows
        Logger.info("SELECT executed successfully, rows: #{inspect(result)}")
        send_select_response(socket, result)
        {:ok, new_storage_state}

      {:ok, {_adapter_module, new_storage_state}} ->
        # INSERT or CREATE TABLE statement - send appropriate response
        case ast do
          %{__struct__: ExDb.SQL.AST.InsertStatement} ->
            Logger.info("INSERT executed successfully")
            send_insert_response(socket)

          %{__struct__: ExDb.SQL.AST.CreateTableStatement} ->
            Logger.info("CREATE TABLE executed successfully")
            send_create_table_response(socket)

          _ ->
            Logger.info("Statement executed successfully")
            # fallback
            send_insert_response(socket)
        end

        {:ok, new_storage_state}

      {:error, {:table_not_found, table_name}} ->
        Logger.warning("Table not found: #{table_name}")
        send_error(socket, "relation \"#{table_name}\" does not exist", "ERROR")
        send_ready_for_query(socket)
        {:ok, elem(adapter, 1)}

      {:error, {:table_already_exists, table_name}} ->
        Logger.warning("Table already exists: #{table_name}")
        send_error(socket, "relation \"#{table_name}\" already exists", "ERROR")
        send_ready_for_query(socket)
        {:ok, elem(adapter, 1)}

      {:error, reason} ->
        Logger.warning("SQL execution failed: #{inspect(reason)}")
        send_error(socket, "execution error: #{inspect(reason)}", "ERROR")
        send_ready_for_query(socket)
        {:ok, elem(adapter, 1)}
    end
  end

  # Send SELECT response with rows
  defp send_select_response(socket, rows) do
    # For now, use a simple column description for SELECT * queries
    # In a real implementation, this would be determined by the table schema
    col_count = if Enum.empty?(rows), do: 0, else: length(List.first(rows))

    columns =
      for i <- 1..col_count do
        %{
          # PostgreSQL uses ?column? for anonymous columns
          name: "?column?",
          table_oid: 0,
          column_attr: i,
          # int4 type (for SELECT 1)
          type_oid: 23,
          type_size: 4,
          type_modifier: -1,
          format_code: 0
        }
      end

    # Send response sequence
    messages = [
      Messages.row_description(columns),
      Enum.map(rows, fn row -> Messages.data_row(Enum.map(row, &to_string/1)) end),
      Messages.command_complete("SELECT #{length(rows)}"),
      Messages.ready_for_query()
    ]

    for msg <- List.flatten(messages) do
      :gen_tcp.send(socket, msg)
    end
  end

  # Send INSERT response
  defp send_insert_response(socket) do
    for msg <- [
          Messages.command_complete("INSERT 0 1"),
          Messages.ready_for_query()
        ] do
      :gen_tcp.send(socket, msg)
    end
  end

  # Send CREATE TABLE response
  defp send_create_table_response(socket) do
    for msg <- [
          Messages.command_complete("CREATE TABLE"),
          Messages.ready_for_query()
        ] do
      :gen_tcp.send(socket, msg)
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
