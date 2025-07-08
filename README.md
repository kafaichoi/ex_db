# ExDb

A simple, educational Postgres-compatible database written in Elixir.

## Project Goal

Build a minimal, understandable database server that speaks the Postgres wire protocol and implements core database features, step by step.

## Roadmap

1. **Wire Protocol with Server**: Accept client connections and speak the Postgres protocol.
2. **SQL Parser**: Parse incoming SQL queries.
3. **In-Memory Storage & Buffer Pool**: Store data in memory with a basic buffer pool.
4. **Query Executor**: Execute parsed queries against the in-memory storage.
5. **Persistence (Disk Storage)**: Add disk-based storage for durability.
6. **Transactions & WAL**: Support transactions and implement Write-Ahead Logging.
7. **B-tree Indexes**: Add indexing for efficient data retrieval.
8. **Advanced Features**: Implement joins and other advanced SQL features.

---

This project is for learning and experimentation. Contributions and questions are welcome!

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ex_db` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_db, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/ex_db>.

