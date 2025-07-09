defmodule ExDb.Server do
  use GenServer

  alias ExDb.Wire.Protocol

  require Logger

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
    case Protocol.handle_startup(socket) do
      {:ok, _params} ->
        # Keep connection open for queries
        handle_queries(socket)

      {:error, :invalid_protocol, _protocol_version} ->
        # Error response was already sent, just close the connection
        :gen_tcp.close(socket)

      {:error, :malformed} ->
        # Malformed packet: just close the connection (no error message sent)
        :gen_tcp.close(socket)
    end
  end

  defp handle_queries(socket) do
    case Protocol.handle_query(socket) do
      :ok ->
        # Query handled successfully, continue listening for more queries
        handle_queries(socket)

      {:error, :closed} ->
        Logger.info("Client closed connection")
        # Client closed connection
        :ok

      {:error, :malformed} ->
        # Malformed query: close the connection
        :gen_tcp.close(socket)
    end
  end
end
