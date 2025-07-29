defmodule ExDb.Wire.Transport do
  @moduledoc """
  Transport layer for PostgreSQL wire protocol.

  Handles sending and receiving messages over TCP sockets, providing a clean
  separation between message representation and transport operations.
  """

  alias ExDb.Wire.Messages
  alias ExDb.Wire.ErrorMessage

  # Transaction status indicators
  @txn_status_idle ?I

  @doc """
  Send an error message to a client socket.

  Takes an ErrorMessage struct and sends it using PostgreSQL wire protocol.
  Automatically sends ReadyForQuery after the error, as expected by clients.
  """
  def send_error_message(socket, %ErrorMessage{} = error_msg) do
    error_packet =
      Messages.error_response(
        error_msg.severity,
        error_msg.code,
        error_msg.message,
        error_msg.detail,
        error_msg.hint,
        error_msg.table_name,
        error_msg.column_name
      )

    :gen_tcp.send(socket, error_packet)

    # Send ReadyForQuery after error (clients expect this)
    send_ready_for_query(socket)
  end

  @doc """
  Send a ready for query message to indicate server is ready for next command.
  """
  def send_ready_for_query(socket, state \\ @txn_status_idle) do
    ready_packet = Messages.ready_for_query(state)
    :gen_tcp.send(socket, ready_packet)
  end

  @doc """
  Send a SELECT query response with rows and column metadata.
  """
  def send_select_response(socket, rows, columns) do
    # Send row description first
    row_desc_packet = Messages.row_description(columns)
    :gen_tcp.send(socket, row_desc_packet)

    # Send each data row
    for row <- rows do
      data_row_packet = Messages.data_row(row)
      :gen_tcp.send(socket, data_row_packet)
    end

    # Send command complete
    command_complete_packet = Messages.command_complete("SELECT #{length(rows)}")
    :gen_tcp.send(socket, command_complete_packet)

    # Send ready for query
    send_ready_for_query(socket)
  end

  @doc """
  Send an INSERT response.
  """
  def send_insert_response(socket) do
    # INSERT 0 1 (OID 0, 1 row affected)
    command_complete_packet = Messages.command_complete("INSERT 0 1")
    :gen_tcp.send(socket, command_complete_packet)
    send_ready_for_query(socket)
  end

  @doc """
  Send a CREATE TABLE response.
  """
  def send_create_table_response(socket) do
    command_complete_packet = Messages.command_complete("CREATE TABLE")
    :gen_tcp.send(socket, command_complete_packet)
    send_ready_for_query(socket)
  end

  @doc """
  Send authentication OK message.
  """
  def send_auth_ok(socket) do
    auth_packet = Messages.authentication_ok()
    :gen_tcp.send(socket, auth_packet)
  end

  @doc """
  Send parameter status messages for client configuration.
  """
  def send_parameter_status(socket, key, value) do
    param_packet = Messages.parameter_status(key, value)
    :gen_tcp.send(socket, param_packet)
  end

  @doc """
  Receive a message from socket with timeout.

  Returns `{:ok, data}` on success, `{:error, reason}` on failure.
  """
  def receive_message(socket, length, timeout \\ 5000) do
    :gen_tcp.recv(socket, length, timeout)
  end

  @doc """
  Close a socket connection gracefully.
  """
  def close_socket(socket) do
    :gen_tcp.close(socket)
  end
end
