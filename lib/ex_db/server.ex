defmodule ExDb.Server do
  use GenServer

  require Logger

  @port 5432
  # Postgres AuthenticationOk message
  @auth_ok <<"R", 0, 0, 0, 8, 0, 0, 0, 0>>

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    {:ok, listen_socket} =
      :gen_tcp.listen(@port, [:binary, packet: :raw, active: false, reuseaddr: true])

    Logger.info("Listening on port #{@port}")

    spawn_link(fn -> accept_loop(listen_socket) end)
    {:ok, %{listen_socket: listen_socket}}
  end

  defp accept_loop(listen_socket) do
    {:ok, socket} = :gen_tcp.accept(listen_socket)
    Logger.info("Accepted connection from #{inspect(socket)}")

    spawn_link(fn -> handle_client(socket) end)
    accept_loop(listen_socket)
  end

  defp handle_client(socket) do
    # Read startup packet (we don't parse it yet)
    case :gen_tcp.recv(socket, 0, 1000) do
      {:ok, _data} ->
        # Send AuthenticationOk
        :gen_tcp.send(socket, @auth_ok)
        :gen_tcp.close(socket)

      _ ->
        :gen_tcp.close(socket)
    end
  end
end
